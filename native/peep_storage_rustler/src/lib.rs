// Never panic; handle our own errors, so we avoid poisoning write locks. Errors
// should raise exceptions and detach :telemetry handlers.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use hashbrown::hash_map::{HashMap, RawEntryMut};
use hashbrown::HashSet;
use parking_lot::{Mutex, RwLock};
use rustler::env::OwnedEnv;
use rustler::sys::{
    enif_get_double, enif_is_identical, enif_make_copy, enif_monotonic_time, enif_system_info,
    ErlNifSysInfo, ErlNifTimeUnit,
};
use rustler::types::map::MapIterator;
use rustler::types::tuple::get_tuple;
use rustler::wrapper::NIF_TERM;
use rustler::{Atom, Encoder, Env, NifMap, Resource, ResourceArc, Term, TermType};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::OnceLock;

mod atoms {
    rustler::atoms! {
        metric_counter = "Elixir.Telemetry.Metrics.Counter",
        metric_sum = "Elixir.Telemetry.Metrics.Sum",
        metric_last_value = "Elixir.Telemetry.Metrics.LastValue",
        metric_distribution = "Elixir.Telemetry.Metrics.Distribution",
        peep_bucket_boundaries,
        infinity,
        sum,
        peep_storage_error,
        bad_tags_map,
        bad_argument,
        already_registered,
        not_registered,
        unknown_metric_id,
        bad_tag_index,
        bad_measurement,
        unsorted_boundaries,
        map_build_failed,
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                   unsafe                                  //
///////////////////////////////////////////////////////////////////////////////

fn scheduler_count() -> usize {
    let mut info: ErlNifSysInfo = unsafe { std::mem::zeroed() };
    unsafe {
        enif_system_info(&mut info, size_of::<ErlNifSysInfo>());
    }
    info.scheduler_threads as usize
}

fn monotonic_time_ns() -> i64 {
    unsafe { enif_monotonic_time(ErlNifTimeUnit::ERL_NIF_NSEC) }
}

struct TagsEnv(OwnedEnv);

// SAFETY: the shard's `RwLock` guards this. `store` is the only operation that
// allocates and takes `&mut self`, so it is reachable only through the write
// guard; no stored term is ever read outside a guard.
unsafe impl Sync for TagsEnv {}

impl TagsEnv {
    fn new() -> Self {
        TagsEnv(OwnedEnv::new())
    }

    fn store(&mut self, term: NIF_TERM) -> NIF_TERM {
        self.0
            .run(|owned| unsafe { enif_make_copy(owned.as_c_arg(), term) })
    }

    fn copy_out<'a>(&self, env: Env<'a>, term: NIF_TERM) -> Term<'a> {
        unsafe { Term::new(env, enif_make_copy(env.as_c_arg(), term)) }
    }

    fn copy_key(&self, env: Env, key: &TagsKey) -> TagsKey {
        TagsKey {
            hash: key.hash,
            term: self.copy_out(env, key.term).as_c_arg(),
        }
    }

    fn size_of(&self, term: NIF_TERM) -> usize {
        self.0.run(|owned| unsafe { Term::new(owned, term) }.size())
    }
}

fn terms_identical(a: NIF_TERM, b: NIF_TERM) -> bool {
    unsafe { enif_is_identical(a, b) == 1 }
}

/// Wraps rather than copies: sound only for a key `TagsEnv::copy_key` produced
/// for this same `env`. A key still owned by a shard needs `TagsEnv::copy_out`.
fn tags_term<'a>(env: Env<'a>, key: &TagsKey) -> Term<'a> {
    unsafe { Term::new(env, key.term) }
}

// Bypass Rustler's integer-to-float coercion.
fn term_as_f64(term: Term) -> Option<f64> {
    let mut value = 0f64;
    let found =
        unsafe { enif_get_double(term.get_env().as_c_arg(), term.as_c_arg(), &mut value) } == 1;
    found.then_some(value)
}

struct TupleElements<'a> {
    env: Env<'a>,
    elements: &'a [NIF_TERM],
}

impl<'a> TupleElements<'a> {
    fn new(tuple: Term<'a>) -> Option<Self> {
        let env = tuple.get_env();
        let elements =
            unsafe { rustler::wrapper::tuple::get_tuple(env.as_c_arg(), tuple.as_c_arg()) }.ok()?;
        Some(TupleElements { env, elements })
    }

    fn get(&self, index: usize) -> Option<Term<'a>> {
        let element = *self.elements.get(index)?;
        Some(unsafe { Term::new(self.env, element) })
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                   types                                   //
///////////////////////////////////////////////////////////////////////////////

struct Storage {
    registered: OnceLock<RegisteredMetrics>,
}

#[rustler::resource_impl]
impl Resource for Storage {}

// parking_lot doesn't poison, so it can't assert this for us. Nothing fallible
// runs inside a critical section, so a guard can't drop mid-unwind.
impl std::panic::RefUnwindSafe for Storage {}

struct RegisteredMetrics {
    metrics: Vec<MetricSlot>,
    boundaries_arena: Box<[Measurement]>,
}

enum MetricSlot {
    Counter(Shards<AtomicI64>),
    Sum(Shards<AtomicI64>),
    LastValue(Shards<LastValueCell>),
    Distribution {
        boundaries: (usize, usize),
        shards: Shards<DistributionCell>,
    },
}

// Matches crossbeam_utils::CachePadded for x86_64 and aarch64
#[repr(align(128))]
struct CachePadded<T>(T);

type ShardMap<V> = HashMap<TagsKey, V, TermHashBuilder>;

/// One `RwLock` over both: see the SAFETY note on `TagsEnv`.
struct Shard<V> {
    map: ShardMap<V>,
    tags: TagsEnv,
}

struct Shards<V> {
    shards: Box<[CachePadded<RwLock<Shard<V>>>]>,
}

const _: () = assert!(size_of::<RwLock<Shard<AtomicI64>>>() <= 128);

/// A `last_value` timestamp and measurement.
type Sample = (i64, Measurement);
type LastValueCell = Mutex<Sample>;

/// Timestamp ties use numeric order, then prefer floats and positive zero.
/// This makes the winner independent of shard and insertion order.
fn newer(sample: Sample, current: Sample) -> bool {
    sample
        .0
        .cmp(&current.0)
        .then_with(|| sample.1.term_cmp(current.1))
        .is_gt()
}

struct DistributionCell {
    buckets: Box<[AtomicU64]>,
    sum: AtomicI64,
}

///////////////////////////////////////////////////////////////////////////////
//                                   errors                                  //
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum StorageError {
    BadTagsMap,
    BadArgument(&'static str),
    BadMeasurement(&'static str),
    MapBuildFailed(&'static str),
    UnknownMetricId(usize),
    BadTagIndex(usize),
    UnsortedBoundaries,
    AlreadyRegistered,
    NotRegistered,
}

impl StorageError {
    fn reason(&self) -> Atom {
        match self {
            StorageError::BadTagsMap => atoms::bad_tags_map(),
            StorageError::BadArgument(_) => atoms::bad_argument(),
            StorageError::BadMeasurement(_) => atoms::bad_measurement(),
            StorageError::MapBuildFailed(_) => atoms::map_build_failed(),
            StorageError::UnknownMetricId(_) => atoms::unknown_metric_id(),
            StorageError::BadTagIndex(_) => atoms::bad_tag_index(),
            StorageError::UnsortedBoundaries => atoms::unsorted_boundaries(),
            StorageError::AlreadyRegistered => atoms::already_registered(),
            StorageError::NotRegistered => atoms::not_registered(),
        }
    }

    fn detail(&self) -> String {
        match self {
            StorageError::BadTagsMap => "tags must be a map".into(),
            StorageError::BadArgument(detail) => (*detail).into(),
            StorageError::BadMeasurement(detail) => (*detail).into(),
            StorageError::MapBuildFailed(detail) => (*detail).into(),
            StorageError::UnknownMetricId(id) => format!("no metric registered for id {id}"),
            StorageError::BadTagIndex(idx) => format!("no tags map at index {idx}"),
            StorageError::UnsortedBoundaries => {
                "Peep.Buckets.boundaries/1 must return ascending values".into()
            }
            StorageError::AlreadyRegistered => "register_metrics was already called".into(),
            StorageError::NotRegistered => "register_metrics has not been called".into(),
        }
    }
}

impl Encoder for StorageError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        (atoms::peep_storage_error(), self.reason(), self.detail()).encode(env)
    }
}

impl From<StorageError> for rustler::Error {
    fn from(error: StorageError) -> Self {
        rustler::Error::RaiseTerm(Box::new(error))
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                    new                                    //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
fn new(_opts: Term) -> ResourceArc<Storage> {
    ResourceArc::new(Storage {
        registered: OnceLock::new(),
    })
}

///////////////////////////////////////////////////////////////////////////////
//                              register_metrics                             //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif]
fn register_metrics(storage: &Storage, ids_to_metrics: Term) -> Result<Atom, rustler::Error> {
    let n_shards = scheduler_count();
    let mut boundaries_arena: Vec<Measurement> = Vec::new();
    let mut interned: Vec<(Vec<Measurement>, usize)> = Vec::new();

    let metrics = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?
        .into_iter()
        .map(|metric| metric_slot_for(metric, &mut boundaries_arena, &mut interned, n_shards))
        .collect::<Result<_, _>>()?;

    let registered = RegisteredMetrics {
        metrics,
        boundaries_arena: boundaries_arena.into_boxed_slice(),
    };

    storage
        .registered
        .set(registered)
        .map_err(|_| StorageError::AlreadyRegistered)?;

    Ok(rustler::types::atom::ok())
}

fn metric_slot_for(
    metric: Term,
    arena: &mut Vec<Measurement>,
    interned: &mut Vec<(Vec<Measurement>, usize)>,
    n_shards: usize,
) -> Result<MetricSlot, StorageError> {
    let struct_name: Atom = metric
        .map_get(rustler::types::atom::__struct__())
        .map_err(|_| StorageError::BadArgument("metric must be a struct"))?
        .decode()
        .map_err(|_| StorageError::BadArgument("__struct__ must be an atom"))?;

    if struct_name == atoms::metric_counter() {
        Ok(MetricSlot::Counter(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_sum() {
        Ok(MetricSlot::Sum(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_last_value() {
        Ok(MetricSlot::LastValue(Shards::new(n_shards)))
    } else if struct_name == atoms::metric_distribution() {
        // Preserve integer boundaries: f64 loses precision above 2^53.
        let boundaries = metric
            .map_get(atoms::peep_bucket_boundaries())
            .map_err(|_| {
                StorageError::BadArgument("Distribution metric must carry :peep_bucket_boundaries")
            })?
            .decode::<Vec<Term>>()
            .map_err(|_| {
                StorageError::BadArgument(":peep_bucket_boundaries must be a list of numbers")
            })?
            .into_iter()
            .map(Measurement::decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                StorageError::BadArgument(":peep_bucket_boundaries must be a list of numbers")
            })?;

        Ok(MetricSlot::Distribution {
            boundaries: intern_boundaries(arena, interned, boundaries)?,
            shards: Shards::new(n_shards),
        })
    } else {
        Err(StorageError::BadArgument("unrecognized metric struct"))
    }
}

fn intern_boundaries(
    arena: &mut Vec<Measurement>,
    interned: &mut Vec<(Vec<Measurement>, usize)>,
    boundaries: Vec<Measurement>,
) -> Result<(usize, usize), StorageError> {
    if !boundaries.is_sorted_by(|a, b| !a.cmp_exact(*b).is_gt()) {
        return Err(StorageError::UnsortedBoundaries);
    }

    if let Some((existing, offset)) = interned
        .iter()
        .find(|(existing, _)| existing == &boundaries)
    {
        return Ok((*offset, existing.len()));
    }

    let offset = arena.len();
    arena.extend_from_slice(&boundaries);
    let len = boundaries.len();
    interned.push((boundaries, offset));
    Ok((offset, len))
}

///////////////////////////////////////////////////////////////////////////////
//                               insert_metrics                              //
///////////////////////////////////////////////////////////////////////////////
/// `metric` is unused here; it stays in the item shape because the ETS backends
/// dispatch on it.
#[rustler::nif]
fn insert_metrics(
    resolved: (&Storage, usize),
    tag_results: Term,
    batch: Term,
) -> Result<Atom, rustler::Error> {
    let (storage, shard_id) = resolved;
    let registered = storage
        .registered
        .get()
        .ok_or(StorageError::NotRegistered)?;

    let tag_terms = TupleElements::new(tag_results)
        .ok_or(StorageError::BadArgument("tag_results must be a tuple"))?;

    // `u64::MAX` is the "not yet computed" sentinel. A `tag_idx` past the end
    // falls back to hashing per sample.
    let mut hashes = [u64::MAX; 8];
    // Gauges in one event share a timestamp.
    let mut now = None;

    let batch = batch
        .into_list_iterator()
        .map_err(|_| StorageError::BadArgument("batch must be a list"))?;

    for item in batch {
        // Avoids `Vec<Term>` allocations by `rustler::types::tuple::get_tuple`
        let (id, _metric, value, tag_idx): (usize, Term, Term, usize) =
            item.decode().map_err(|_| {
                StorageError::BadArgument("batch items must be {id, metric, value, tag_idx} tuples")
            })?;

        let tags = tag_terms
            .get(tag_idx)
            .ok_or(StorageError::BadTagIndex(tag_idx))?;

        let hash = match hashes.get_mut(tag_idx) {
            Some(cached) => {
                if *cached == u64::MAX {
                    *cached = tags.hash_internal(0);
                }
                *cached
            }
            None => tags.hash_internal(0),
        };

        store_one(registered, shard_id, id, value, tags, hash, &mut now)?;
    }

    Ok(rustler::types::atom::ok())
}

#[inline]
fn store_one(
    registered: &RegisteredMetrics,
    shard_id: usize,
    id: usize,
    value: Term,
    tags: Term,
    hash: u64,
    now: &mut Option<i64>,
) -> Result<(), StorageError> {
    match registered.metrics.get(id) {
        Some(MetricSlot::Counter(shards)) => {
            shards.add(shard_id, tags, hash, 1);
            Ok(())
        }
        Some(MetricSlot::Sum(shards)) => {
            let delta = value
                .decode()
                .map_err(|_| StorageError::BadMeasurement("sum value must be an integer"))?;

            shards.add(shard_id, tags, hash, delta);
            Ok(())
        }
        Some(MetricSlot::LastValue(shards)) => {
            let sample = (
                *now.get_or_insert_with(monotonic_time_ns),
                Measurement::decode(value)?,
            );
            let make = LastValueCell::new;
            let apply = |cell: &LastValueCell, sample: Sample| {
                let mut current = cell.lock();
                if newer(sample, *current) {
                    *current = sample;
                }
            };

            shards.upsert(shard_id, tags, hash, sample, make, apply);
            Ok(())
        }
        Some(MetricSlot::Distribution { boundaries, shards }) => {
            let (offset, len) = *boundaries;
            let boundaries = &registered.boundaries_arena[offset..offset + len];
            let measurement = Measurement::decode(value)?;
            // Resolved before `upsert` can run, so a measurement that is
            // going to be rejected cannot leave an empty series behind.
            let delta = measurement.rounded()?;
            let make = |measurement: Measurement| {
                let cell = DistributionCell::new(len);
                cell.record(boundaries, measurement, delta);
                cell
            };
            let apply = |cell: &DistributionCell, measurement: Measurement| {
                cell.record(boundaries, measurement, delta);
            };

            shards.upsert(shard_id, tags, hash, measurement, make, apply);
            Ok(())
        }
        None => Err(StorageError::UnknownMetricId(id)),
    }
}

///////////////////////////////////////////////////////////////////////////////
//                                storage_size                               //
///////////////////////////////////////////////////////////////////////////////

#[derive(NifMap)]
struct StorageSize {
    size: usize,
    memory: usize,
}

#[rustler::nif(schedule = "DirtyCpu")]
fn storage_size(storage: &Storage) -> StorageSize {
    let Some(registered) = storage.registered.get() else {
        return StorageSize { size: 0, memory: 0 };
    };

    let mut size = 0;
    let mut memory = size_of::<RegisteredMetrics>()
        + registered.boundaries_arena.len() * size_of::<Measurement>();

    for slot in &registered.metrics {
        let (slot_size, slot_memory) = match slot {
            MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                shards_size_and_memory(shards, |_| 0)
            }
            MetricSlot::LastValue(shards) => shards_size_and_memory(shards, |_| 0),
            MetricSlot::Distribution { shards, .. } => {
                shards_size_and_memory(shards, |cell| cell.buckets.len() * size_of::<AtomicU64>())
            }
        };

        size += slot_size;
        memory += slot_memory;
    }

    StorageSize { size, memory }
}

fn shards_size_and_memory<V>(
    shards: &Shards<V>,
    value_heap_size: impl Fn(&V) -> usize + Copy,
) -> (usize, usize) {
    let mut total_size = 0;
    let mut total_memory = shards.len() * size_of::<CachePadded<RwLock<Shard<V>>>>();
    for shard in shards.iter() {
        let (size, memory) = map_size_and_memory(&shard.read(), value_heap_size);
        total_size += size;
        total_memory += memory;
    }
    (total_size, total_memory)
}

fn map_size_and_memory<V>(
    shard: &Shard<V>,
    value_heap_size: impl Fn(&V) -> usize,
) -> (usize, usize) {
    let mut memory = shard.map.allocation_size();
    for (key, value) in shard.map.iter() {
        memory += shard.tags.size_of(key.term) + value_heap_size(value);
    }
    (shard.map.len(), memory)
}

///////////////////////////////////////////////////////////////////////////////
//                              get_all_metrics                              //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyCpu")]
fn nif_get_all_metrics<'a>(
    storage: &Storage,
    ids_to_metrics: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let env = ids_to_metrics.get_env();

    let Some(registered) = storage.registered.get() else {
        return Ok(Term::map_new(env));
    };

    let metric_terms = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?;

    let mut outer_keys = Vec::new();
    let mut outer_vals = Vec::new();

    for (metric_term, slot) in metric_terms.iter().zip(&registered.metrics) {
        let (len, inner) = match slot {
            MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                encode_counter_map(env, shards)?
            }
            MetricSlot::LastValue(shards) => encode_last_value_map(env, shards)?,
            MetricSlot::Distribution { shards, .. } => encode_distribution_map(env, shards)?,
        };

        if len > 0 {
            outer_keys.push(*metric_term);
            outer_vals.push(inner);
        }
    }

    let map = Term::map_from_term_arrays(env, &outer_keys, &outer_vals)
        .map_err(|_| StorageError::MapBuildFailed("duplicate metric definitions"))?;

    Ok(map)
}

fn combined_capacity<V>(shards: &Shards<V>) -> usize {
    shards
        .iter()
        .map(|shard| shard.read().map.len())
        .max()
        .unwrap_or(0)
}

/// The shard's read lock is held for its whole pass, which is what makes
/// `copy_key` sound: a key never leaves its owning shard uncopied.
fn merge_shards<V, A>(
    env: Env,
    shards: &Shards<V>,
    init: impl Fn(&V) -> A,
    merge: impl Fn(&mut A, &V),
) -> HashMap<TagsKey, A, TermHashBuilder> {
    let mut combined =
        HashMap::with_capacity_and_hasher(combined_capacity(shards), TermHashBuilder::default());

    for shard in shards.iter() {
        let shard = shard.read();
        for (key, cell) in shard.map.iter() {
            match combined
                .raw_entry_mut()
                .from_hash(key.hash, |seen| seen == key)
            {
                RawEntryMut::Occupied(mut entry) => merge(entry.get_mut(), cell),
                RawEntryMut::Vacant(entry) => {
                    entry.insert(shard.tags.copy_key(env, key), init(cell));
                }
            }
        }
    }

    combined
}

fn encode_merged<'a, A>(
    env: Env<'a>,
    combined: &HashMap<TagsKey, A, TermHashBuilder>,
    label: &'static str,
    encode: impl Fn(Env<'a>, &A) -> Result<Term<'a>, StorageError>,
) -> Result<(usize, Term<'a>), StorageError> {
    let mut keys = Vec::with_capacity(combined.len());
    let mut vals = Vec::with_capacity(combined.len());

    for (key, value) in combined {
        keys.push(tags_term(env, key));
        vals.push(encode(env, value)?);
    }

    let map = Term::map_from_term_arrays(env, &keys, &vals)
        .map_err(|_| StorageError::MapBuildFailed(label))?;

    Ok((combined.len(), map))
}

fn encode_counter_map<'a>(
    env: Env<'a>,
    counters: &Shards<AtomicI64>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        counters,
        |cell: &AtomicI64| cell.load(Ordering::Relaxed),
        |total, cell| *total = total.wrapping_add(cell.load(Ordering::Relaxed)),
    );

    encode_merged(env, &combined, "counter map", |env, total| {
        Ok(total.encode(env))
    })
}

fn encode_last_value_map<'a>(
    env: Env<'a>,
    last_values: &Shards<LastValueCell>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        last_values,
        |cell: &LastValueCell| *cell.lock(),
        |newest, cell| {
            let sample = *cell.lock();
            if newer(sample, *newest) {
                *newest = sample;
            }
        },
    );

    encode_merged(env, &combined, "last_value map", |env, (_, value)| {
        Ok(value.encode(env))
    })
}

fn encode_distribution_map<'a>(
    env: Env<'a>,
    distributions: &Shards<DistributionCell>,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_shards(
        env,
        distributions,
        |cell: &DistributionCell| {
            let buckets: Vec<u64> = cell
                .buckets
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect();
            (buckets, cell.sum.load(Ordering::Relaxed))
        },
        |(buckets, sum), cell| {
            for (bucket, cell) in buckets.iter_mut().zip(cell.buckets.iter()) {
                *bucket = bucket.wrapping_add(cell.load(Ordering::Relaxed));
            }
            *sum = sum.wrapping_add(cell.sum.load(Ordering::Relaxed));
        },
    );

    encode_merged(env, &combined, "distribution map", |env, (buckets, sum)| {
        encode_distribution_buckets(env, buckets, *sum)
    })
}

fn encode_distribution_buckets<'a>(
    env: Env<'a>,
    buckets: &[u64],
    sum: i64,
) -> Result<Term<'a>, StorageError> {
    let overflow_idx = buckets.len().saturating_sub(1);
    let mut keys = Vec::with_capacity(buckets.len() + 1);
    let mut vals = Vec::with_capacity(buckets.len() + 1);

    for (idx, count) in buckets.iter().enumerate() {
        let key = if idx == overflow_idx {
            atoms::infinity().encode(env)
        } else {
            (idx as u64).encode(env)
        };
        keys.push(key);
        vals.push(count.encode(env));
    }

    keys.push(atoms::sum().encode(env));
    vals.push(sum.encode(env));

    Term::map_from_term_arrays(env, &keys, &vals)
        .map_err(|_| StorageError::MapBuildFailed("distribution bucket map"))
}

///////////////////////////////////////////////////////////////////////////////
//                                 prune_tags                                //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyCpu")]
fn prune_tags(storage: &Storage, patterns: Term) -> Result<Atom, rustler::Error> {
    if let Some(registered) = storage.registered.get() {
        let env = patterns.get_env();
        let patterns: Vec<Term> = patterns
            .decode()
            .map_err(|_| StorageError::BadArgument("patterns must be a list"))?;

        if patterns.iter().any(|pattern| !pattern.is_map()) {
            return Err(StorageError::BadTagsMap.into());
        }

        for slot in &registered.metrics {
            match slot {
                MetricSlot::Counter(shards) | MetricSlot::Sum(shards) => {
                    prune_shards(env, shards, &patterns)
                }
                MetricSlot::LastValue(shards) => prune_shards(env, shards, &patterns),
                MetricSlot::Distribution { shards, .. } => prune_shards(env, shards, &patterns),
            }
        }
    }

    Ok(rustler::types::atom::ok())
}

fn prune_shards<'a, V>(env: Env<'a>, shards: &Shards<V>, patterns: &[Term<'a>]) {
    for shard_lock in shards.iter() {
        // Matched under the read lock.
        let doomed: HashSet<NIF_TERM> = {
            let shard = shard_lock.read();
            shard
                .map
                .keys()
                .filter(|key| matches_any_pattern(env, &shard.tags, key, patterns))
                .map(|key| key.term)
                .collect()
        };

        if doomed.is_empty() {
            continue;
        }

        // An environment has no per-term free, so reclaiming means copying the
        // survivors into a fresh one and dropping the old.
        let mut shard = shard_lock.write();
        let Shard { map, tags } = &mut *shard;

        let mut fresh = TagsEnv::new();
        let mut kept: ShardMap<V> =
            HashMap::with_capacity_and_hasher(map.len(), TermHashBuilder::default());

        for (key, value) in map.drain() {
            if doomed.contains(&key.term) {
                continue;
            }

            let moved = TagsKey {
                hash: key.hash,
                term: fresh.store(key.term),
            };

            if let RawEntryMut::Vacant(entry) = kept
                .raw_entry_mut()
                .from_hash(moved.hash, |seen| seen == &moved)
            {
                entry.insert(moved, value);
            }
        }

        *map = kept;
        *tags = fresh;
    }
}

fn matches_any_pattern<'a>(
    env: Env<'a>,
    tags_env: &TagsEnv,
    key: &TagsKey,
    patterns: &[Term<'a>],
) -> bool {
    let tags = tags_env.copy_out(env, key.term);

    patterns.iter().any(|pattern| {
        MapIterator::new(*pattern).is_some_and(|mut pairs| {
            pairs.all(|(name, value)| tags.map_get(name).is_ok_and(|found| found == value))
        })
    })
}

///////////////////////////////////////////////////////////////////////////////
//                                   shards                                  //
///////////////////////////////////////////////////////////////////////////////

impl<V> Shards<V> {
    fn new(n_shards: usize) -> Self {
        let shards = (0..n_shards.max(1))
            .map(|_| {
                CachePadded(RwLock::new(Shard {
                    map: HashMap::default(),
                    tags: TagsEnv::new(),
                }))
            })
            .collect();
        Shards { shards }
    }

    fn iter(&self) -> impl Iterator<Item = &RwLock<Shard<V>>> {
        self.shards.iter().map(|shard| &shard.0)
    }

    fn len(&self) -> usize {
        self.shards.len()
    }

    // `resolve/1` hands back `scheduler_id - 1`, and there is one shard per
    // scheduler.
    fn shard(&self, shard_id: usize) -> &RwLock<Shard<V>> {
        &self.shards[shard_id].0
    }

    /// Exactly one of `make` and `apply` runs.
    fn upsert<S>(
        &self,
        shard_id: usize,
        tags: Term,
        hash: u64,
        sample: S,
        make: impl FnOnce(S) -> V,
        apply: impl FnOnce(&V, S),
    ) {
        let shard_lock = self.shard(shard_id);

        // Fast path.
        {
            let shard = shard_lock.read();
            if let Some((_, value)) = shard
                .map
                .raw_entry()
                .from_hash(hash, |key| key.matches(tags))
            {
                apply(value, sample);
                return;
            }
        }

        // Slow path: the copy allocates into the shard's environment.
        let mut shard = shard_lock.write();
        let Shard { map, tags: env } = &mut *shard;

        match map.raw_entry_mut().from_hash(hash, |key| key.matches(tags)) {
            RawEntryMut::Occupied(entry) => apply(entry.into_mut(), sample),
            RawEntryMut::Vacant(entry) => {
                let key = TagsKey {
                    hash,
                    term: env.store(tags.as_c_arg()),
                };
                entry.insert(key, make(sample));
            }
        }
    }
}

impl Shards<AtomicI64> {
    #[inline]
    fn add(&self, shard_id: usize, tags: Term, hash: u64, delta: i64) {
        self.upsert(
            shard_id,
            tags,
            hash,
            delta,
            AtomicI64::new,
            |counter: &AtomicI64, delta| {
                counter.fetch_add(delta, Ordering::Relaxed);
            },
        );
    }
}

impl DistributionCell {
    fn new(num_boundaries: usize) -> Self {
        let buckets: Box<[AtomicU64]> = (0..=num_boundaries).map(|_| AtomicU64::new(0)).collect();
        DistributionCell {
            buckets,
            sum: AtomicI64::new(0),
        }
    }

    fn record(&self, boundaries: &[Measurement], value: Measurement, delta: i64) {
        let idx = boundaries.partition_point(|boundary| !boundary.cmp_exact(value).is_gt());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(delta, Ordering::Relaxed);
    }
}

///////////////////////////////////////////////////////////////////////////////
//                               caching tags                                //
///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct TermPassthroughHasher(u64);

impl Hasher for TermPassthroughHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("TagsKey is the only key type, and it hashes via write_u64")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type TermHashBuilder = BuildHasherDefault<TermPassthroughHasher>;

/// `term` lives in the owning shard's `TagsEnv` and is valid only while that
/// shard's lock is held.
#[derive(Clone, Copy)]
struct TagsKey {
    hash: u64,
    term: NIF_TERM,
}

impl TagsKey {
    fn matches(&self, tags: Term) -> bool {
        terms_identical(self.term, tags.as_c_arg())
    }
}

impl PartialEq for TagsKey {
    fn eq(&self, other: &Self) -> bool {
        terms_identical(self.term, other.term)
    }
}
impl Eq for TagsKey {}

impl Hash for TagsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// 2^63: exactly representable and the first `f64` above `i64::MAX`.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// Compare without rounding `int` to f64; guards also handle non-finite floats.
fn cmp_int_float(int: i64, float: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    if float.is_nan() {
        return Ordering::Greater;
    }
    if float >= TWO_POW_63 {
        return Ordering::Less;
    }
    if float < -TWO_POW_63 {
        return Ordering::Greater;
    }

    // Truncation avoids a floor/libm call on baseline x86-64; range guards
    // prevent saturating conversion.
    let trunc = float as i64;

    match int.cmp(&trunc) {
        // Equal integer parts: compare the signed fractional remainder.
        Ordering::Equal => (trunc as f64)
            .partial_cmp(&float)
            .unwrap_or(Ordering::Equal),
        ordering => ordering,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Measurement {
    Int(i64),
    Float(f64),
}

impl Measurement {
    /// Decode directly; query the type only to distinguish overflow from non-numbers.
    fn decode(term: Term) -> Result<Self, StorageError> {
        if let Ok(int) = term.decode::<i64>() {
            return Ok(Measurement::Int(int));
        }

        if let Some(float) = term_as_f64(term) {
            return Ok(Measurement::Float(float));
        }

        Err(match term.get_type() {
            TermType::Integer => StorageError::BadMeasurement("integer does not fit in 64 bits"),
            _ => StorageError::BadMeasurement("measurement must be a number"),
        })
    }

    /// Match :atomics' signed 64-bit range; reject overflow before Rust's
    /// saturating float-to-int cast.
    fn rounded(self) -> Result<i64, StorageError> {
        match self {
            Measurement::Int(int) => Ok(int),
            Measurement::Float(float) => {
                let rounded = float.round();

                if rounded.is_finite() && (-TWO_POW_63..TWO_POW_63).contains(&rounded) {
                    Ok(rounded as i64)
                } else {
                    Err(StorageError::BadMeasurement(
                        "distribution value does not fit in 64 bits",
                    ))
                }
            }
        }
    }

    /// Numeric comparison without losing integer precision above 2^53.
    fn cmp_exact(self, other: Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (Measurement::Int(a), Measurement::Int(b)) => a.cmp(&b),
            (Measurement::Float(a), Measurement::Float(b)) => {
                a.partial_cmp(&b).unwrap_or(Ordering::Equal)
            }
            (Measurement::Int(a), Measurement::Float(b)) => cmp_int_float(a, b),
            (Measurement::Float(a), Measurement::Int(b)) => cmp_int_float(b, a).reverse(),
        }
    }

    fn type_rank(self) -> u8 {
        match self {
            Measurement::Int(_) => 0,
            Measurement::Float(_) => 1,
        }
    }

    /// Unlike bucket comparisons, last_value ties distinguish representations.
    fn term_cmp(self, other: Self) -> std::cmp::Ordering {
        self.cmp_exact(other)
            .then_with(|| self.type_rank().cmp(&other.type_rank()))
            .then_with(|| match (self, other) {
                (Measurement::Float(a), Measurement::Float(b)) => a.total_cmp(&b),
                _ => std::cmp::Ordering::Equal,
            })
    }
}

impl Encoder for Measurement {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            Measurement::Int(i) => i.encode(env),
            Measurement::Float(f) => f.encode(env),
        }
    }
}

rustler::init!("Elixir.Peep.Storage.RustNIF");
