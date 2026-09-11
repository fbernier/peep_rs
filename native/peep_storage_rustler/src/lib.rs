// Prefer StorageError exceptions to :nif_panicked; either can detach telemetry.
// clippy::panic does not cover the other panicking macros.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing
)]

use hashbrown::hash_map::{Entry, HashMap, RawEntryMut};
use parking_lot::RwLock;
use rustler::env::OwnedEnv;
use rustler::sys::{
    enif_get_double, enif_is_identical, enif_make_copy, enif_monotonic_time, enif_system_info,
    ErlNifSysInfo, ErlNifTimeUnit,
};
use rustler::types::map::MapIterator;
use rustler::types::tuple::get_tuple;
use rustler::wrapper::NIF_TERM;
use rustler::{Atom, Encoder, Env, NifMap, Resource, ResourceArc, Term, TermType};
use std::cell::Cell;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

mod atoms {
    rustler::atoms! {
        metric_counter = "Elixir.Telemetry.Metrics.Counter",
        metric_sum = "Elixir.Telemetry.Metrics.Sum",
        metric_last_value = "Elixir.Telemetry.Metrics.LastValue",
        metric_distribution = "Elixir.Telemetry.Metrics.Distribution",
        peep_bucket_boundaries,
        peep_bucket_labels,
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
        metrics_mismatch,
        too_many_tag_sets,
        no_shards,
        map_build_failed,
        invariant_violation,
        contended,
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

/// Resolve the normal scheduler here: a process can migrate before the NIF.
/// Dirty continuations reuse this shard index rather than the dirty thread's.
fn thread_shard() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static ASSIGNED: Cell<usize> = const { Cell::new(usize::MAX) };
    }

    ASSIGNED.with(|assigned| {
        let mut shard = assigned.get();

        if shard == usize::MAX {
            shard = NEXT.fetch_add(1, Ordering::Relaxed);
            assigned.set(shard);
        }

        shard
    })
}

struct TagsEnv(OwnedEnv);

// SAFETY: shard environments are guarded by the shard's RwLock; labels_env
// is immutable after OnceLock publication. Only store allocates, via &mut self
// under a write guard or during registration. All other access is read-only.
unsafe impl Sync for TagsEnv {}

/// Per-environment overhead excluding terms: 862–870 bytes on OTP 29/x86_64,
/// measured from the `erlang:memory` delta across allocated environments.
const ENV_OVERHEAD_BYTES: usize = 870;

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

    /// Both `patterns` and `term` must belong to this environment.
    fn matches_any(&self, patterns: &[NIF_TERM], term: NIF_TERM) -> bool {
        self.0.run(|env| {
            let tags = unsafe { Term::new(env, term) };

            patterns.iter().any(|pattern| {
                let pattern = unsafe { Term::new(env, *pattern) };

                MapIterator::new(pattern).is_some_and(|mut pairs| {
                    pairs.all(|(name, value)| tags.map_get(name).is_ok_and(|found| found == value))
                })
            })
        })
    }

    fn size_of(&self, term: NIF_TERM) -> usize {
        self.0.run(|owned| unsafe { Term::new(owned, term) }.size())
    }
}

fn terms_identical(a: NIF_TERM, b: NIF_TERM) -> bool {
    a == b || unsafe { enif_is_identical(a, b) == 1 }
}

/// # Safety
/// Every raw term must belong to `env` and remain valid for this NIF call.
/// The VM consumes the arrays synchronously; it does not retain their storage.
unsafe fn map_from_raw_arrays<'a>(
    env: Env<'a>,
    keys: &[NIF_TERM],
    vals: &[NIF_TERM],
    label: &'static str,
) -> Result<Term<'a>, StorageError> {
    if keys.len() != vals.len() {
        return Err(StorageError::MapBuildFailed(label));
    }

    // The wrapper uses keys.len() for both arrays; check lengths before FFI.
    unsafe {
        rustler::wrapper::map::make_map_from_arrays(env.as_c_arg(), keys, vals)
            .map(|map| Term::new(env, map))
            .ok_or(StorageError::MapBuildFailed(label))
    }
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

// parking_lot does not poison locks or implement RefUnwindSafe.
// Inserts may commit a valid batch prefix; pruning validates IDs before remapping.
impl std::panic::RefUnwindSafe for Storage {}

struct RegisteredMetrics {
    /// Immutable specs shared by every shard.
    metrics: Vec<MetricSpec>,
    shards: Box<[CachePadded<RwLock<Shard>>]>,
    boundaries_arena: Box<[Measurement]>,
    /// Sorted bucket keys plus `:sum`, owned by `labels_env` and immutable
    /// after OnceLock publication.
    labels_arena: Box<[NIF_TERM]>,
    /// Bucket index or `SUM_SLOT` for each key in `labels_arena`.
    order_arena: Box<[u32]>,
    labels_env: TagsEnv,
    label_term_bytes: usize,
}

impl RegisteredMetrics {
    /// Thread IDs can exceed the shard count; modulo keeps them usable as hints.
    /// Registration always creates at least one shard.
    fn shard(&self, shard_id: usize) -> Option<&RwLock<Shard>> {
        let index = shard_id.checked_rem(self.shards.len())?;
        self.shards.get(index).map(|padded| &padded.0)
    }
}

/// A slice of a registry arena.
#[derive(Clone, Copy)]
struct Span {
    offset: usize,
    len: usize,
}

impl Span {
    fn of<T>(self, arena: &[T]) -> Result<&[T], StorageError> {
        let end = self
            .offset
            .checked_add(self.len)
            .ok_or(StorageError::InvariantViolation("arena span end overflows"))?;
        arena
            .get(self.offset..end)
            .ok_or(StorageError::InvariantViolation(
                "arena does not cover span",
            ))
    }
}

enum MetricSpec {
    Counter,
    Sum,
    LastValue,
    Distribution {
        boundaries: Span,
        /// One label per bucket, plus `:sum`.
        labels: Span,
    },
}

impl MetricSpec {
    fn empty_data(&self) -> MetricData {
        match self {
            MetricSpec::Counter => MetricData::Counter(IdMap::default()),
            MetricSpec::Sum => MetricData::Sum(IdMap::default()),
            MetricSpec::LastValue => MetricData::LastValue(IdMap::default()),
            MetricSpec::Distribution { boundaries, .. } => MetricData::Distribution {
                boundaries: *boundaries,
                map: IdMap::default(),
            },
        }
    }

    fn describes(&self, metric: Term) -> bool {
        let expected = match self {
            MetricSpec::Counter => atoms::metric_counter(),
            MetricSpec::Sum => atoms::metric_sum(),
            MetricSpec::LastValue => atoms::metric_last_value(),
            MetricSpec::Distribution { .. } => atoms::metric_distribution(),
        };

        metric
            .map_get(rustler::types::atom::__struct__())
            .and_then(Term::decode::<Atom>)
            .is_ok_and(|struct_name| struct_name == expected)
    }
}

/// Reserved `order_arena` entry for `:sum`.
const SUM_SLOT: u32 = u32::MAX;

// Matches crossbeam_utils::CachePadded for x86_64 and aarch64
#[repr(align(128))]
struct CachePadded<T>(T);

///////////////////////////////////////////////////////////////////////////////
//                                   shards                                  //
///////////////////////////////////////////////////////////////////////////////

/// One lock covers tag IDs and metric data for inserts, scrapes and pruning.
struct Shard {
    tags: TagTable,
    /// Indexed by metric id, parallel to `RegisteredMetrics::metrics`.
    metrics: Box<[MetricData]>,
}

/// The tags maps seen on one scheduler, stored once and shared by every metric.
struct TagTable {
    env: TagsEnv,
    index: HashMap<TagsKey, TagId, TermHashBuilder>,
    /// Reverse lookup by `TagId`.
    keys: Vec<TagsKey>,
    /// Cached term bytes; usize::MAX marks a membership change.
    term_bytes: AtomicUsize,
    /// Bumped when pruning removes IDs; interning only appends.
    generation: u64,
}

/// Identifies a tags map within one shard. Ids are shard-local and dense.
type TagId = u32;

/// Reserved per-batch cache sentinel; never assigned to a tags map.
const UNINTERNED: TagId = TagId::MAX;

type IdMap<V> = HashMap<TagId, V, IdHashBuilder>;

enum MetricData {
    Counter(IdMap<i64>),
    Sum(IdMap<i64>),
    LastValue(IdMap<Sample>),
    Distribution {
        boundaries: Span,
        map: IdMap<Buckets>,
    },
}

const _: () = assert!(size_of::<RwLock<Shard>>() <= 128);

impl TagTable {
    fn new() -> Self {
        TagTable {
            env: TagsEnv::new(),
            index: HashMap::default(),
            keys: Vec::new(),
            term_bytes: AtomicUsize::new(0),
            generation: 0,
        }
    }

    fn term_bytes(&self) -> usize {
        let cached = self.term_bytes.load(Ordering::Relaxed);
        if cached != usize::MAX {
            return cached;
        }
        let bytes = self.keys.iter().map(|key| self.env.size_of(key.term)).sum();
        // Called under the shard's read lock; concurrent readers compute the same sum.
        self.term_bytes.store(bytes, Ordering::Relaxed);
        bytes
    }

    fn intern(&mut self, tags: Term) -> Result<TagId, StorageError> {
        if let [key] = self.keys.as_slice() {
            if key.matches(tags) {
                return Ok(0);
            }
        }
        let hash = tags.hash_internal(0);
        let TagTable {
            env,
            index,
            keys,
            term_bytes,
            ..
        } = self;

        match index
            .raw_entry_mut()
            .from_hash(hash, |key| key.matches(tags))
        {
            RawEntryMut::Occupied(entry) => Ok(*entry.get()),
            RawEntryMut::Vacant(entry) => {
                if keys.len() >= UNINTERNED as usize {
                    return Err(StorageError::TooManyTagSets);
                }

                let key = TagsKey {
                    hash,
                    term: env.store(tags.as_c_arg()),
                };
                let id = keys.len() as TagId;
                keys.push(key);
                entry.insert(key, id);
                term_bytes.store(usize::MAX, Ordering::Relaxed);
                Ok(id)
            }
        }
    }
}

/// A `last_value` timestamp and measurement.
type Sample = (i64, Measurement);

/// Timestamp ties use numeric order, then prefer floats and positive zero.
/// This makes the winner independent of shard and insertion order.
fn newer(sample: Sample, current: Sample) -> bool {
    sample
        .0
        .cmp(&current.0)
        .then_with(|| sample.1.term_cmp(current.1))
        .is_gt()
}

struct Buckets {
    counts: Box<[u64]>,
    sum: i64,
}

impl Buckets {
    fn new(num_boundaries: usize) -> Self {
        Buckets {
            counts: vec![0; num_boundaries.saturating_add(1)].into_boxed_slice(),
            sum: 0,
        }
    }

    /// Validate bucket shape before changing either count or sum.
    fn record(
        &mut self,
        boundaries: &[Measurement],
        value: Measurement,
        delta: i64,
    ) -> Result<(), StorageError> {
        if self.counts.len().checked_sub(1) != Some(boundaries.len()) {
            return Err(StorageError::InvariantViolation(
                "bucket count does not match boundaries",
            ));
        }
        let idx = boundaries.partition_point(|boundary| !boundary.cmp_exact(value).is_gt());
        let count = self
            .counts
            .get_mut(idx)
            .ok_or(StorageError::InvariantViolation(
                "recorded bucket is missing",
            ))?;

        *count = count.wrapping_add(1);
        self.sum = self.sum.wrapping_add(delta);
        Ok(())
    }
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
    InvariantViolation(&'static str),
    UnknownMetricId(usize),
    BadTagIndex(usize),
    UnsortedBoundaries,
    MetricsLenMismatch { got: usize, want: usize },
    MetricKindMismatch { id: usize },
    TooManyTagSets,
    AlreadyRegistered,
    NotRegistered,
    NoShards,
}

impl StorageError {
    fn reason(&self) -> Atom {
        match self {
            StorageError::BadTagsMap => atoms::bad_tags_map(),
            StorageError::BadArgument(_) => atoms::bad_argument(),
            StorageError::BadMeasurement(_) => atoms::bad_measurement(),
            StorageError::MapBuildFailed(_) => atoms::map_build_failed(),
            StorageError::InvariantViolation(_) => atoms::invariant_violation(),
            StorageError::UnknownMetricId(_) => atoms::unknown_metric_id(),
            StorageError::BadTagIndex(_) => atoms::bad_tag_index(),
            StorageError::UnsortedBoundaries => atoms::unsorted_boundaries(),
            StorageError::MetricsLenMismatch { .. } => atoms::metrics_mismatch(),
            StorageError::MetricKindMismatch { .. } => atoms::metrics_mismatch(),
            StorageError::TooManyTagSets => atoms::too_many_tag_sets(),
            StorageError::AlreadyRegistered => atoms::already_registered(),
            StorageError::NotRegistered => atoms::not_registered(),
            StorageError::NoShards => atoms::no_shards(),
        }
    }

    fn detail(&self) -> String {
        match self {
            StorageError::BadTagsMap => "tags must be a map".into(),
            StorageError::BadArgument(detail) => (*detail).into(),
            StorageError::BadMeasurement(detail) => (*detail).into(),
            StorageError::MapBuildFailed(detail) => (*detail).into(),
            StorageError::InvariantViolation(detail) => (*detail).into(),
            StorageError::UnknownMetricId(id) => format!("no metric registered for id {id}"),
            StorageError::BadTagIndex(idx) => format!("no tags map at index {idx}"),
            StorageError::UnsortedBoundaries => {
                "Peep.Buckets.boundaries/1 must return strictly ascending values".into()
            }
            StorageError::MetricsLenMismatch { got, want } => {
                format!("ids_to_metrics has {got} metrics, but {want} were registered")
            }
            StorageError::MetricKindMismatch { id } => {
                format!("the metric at index {id} is not the kind registered there")
            }
            StorageError::TooManyTagSets => {
                format!("a scheduler cannot hold more than {UNINTERNED} distinct tags maps")
            }
            StorageError::AlreadyRegistered => "register_metrics was already called".into(),
            StorageError::NotRegistered => "register_metrics has not been called".into(),
            StorageError::NoShards => "the registry holds no shards".into(),
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

// Registration copies labels and allocates per-metric, per-scheduler state.
#[rustler::nif(schedule = "DirtyCpu")]
fn nif_register_metrics(storage: &Storage, ids_to_metrics: Term) -> Result<Atom, rustler::Error> {
    let mut registration = Registration::new();

    let metric_terms = get_tuple(ids_to_metrics)
        .map_err(|_| StorageError::BadArgument("ids_to_metrics must be a tuple"))?;

    // Duplicate metric keys would make map construction fail on every scrape.
    let mut seen: HashMap<TagsKey, (), TermHashBuilder> =
        HashMap::with_capacity_and_hasher(metric_terms.len(), TermHashBuilder::default());

    for metric in &metric_terms {
        let key = TagsKey {
            hash: metric.hash_internal(0),
            term: metric.as_c_arg(),
        };

        match seen
            .raw_entry_mut()
            .from_hash(key.hash, |seen| *seen == key)
        {
            RawEntryMut::Occupied(_) => {
                return Err(
                    StorageError::BadArgument("ids_to_metrics must not repeat a metric").into(),
                )
            }
            RawEntryMut::Vacant(entry) => {
                entry.insert(key, ());
            }
        }
    }

    let metrics = metric_terms
        .into_iter()
        .map(|metric| registration.spec_for(metric))
        .collect::<Result<_, _>>()?;

    storage
        .registered
        .set(registration.finish(metrics))
        .map_err(|_| StorageError::AlreadyRegistered)?;

    Ok(rustler::types::atom::ok())
}

struct Registration {
    boundaries: Vec<Measurement>,
    /// Deduplicated boundary lists in the arena.
    interned: Vec<Span>,
    labels: Vec<NIF_TERM>,
    order: Vec<u32>,
    labels_env: TagsEnv,
}

impl Registration {
    fn new() -> Self {
        Registration {
            boundaries: Vec::new(),
            interned: Vec::new(),
            labels: Vec::new(),
            order: Vec::new(),
            labels_env: TagsEnv::new(),
        }
    }

    fn finish(self, metrics: Vec<MetricSpec>) -> RegisteredMetrics {
        let shards = (0..scheduler_count().max(1))
            .map(|_| {
                CachePadded(RwLock::new(Shard {
                    tags: TagTable::new(),
                    metrics: metrics.iter().map(MetricSpec::empty_data).collect(),
                }))
            })
            .collect();
        let label_term_bytes = self
            .labels
            .iter()
            .map(|label| self.labels_env.size_of(*label))
            .sum();

        RegisteredMetrics {
            metrics,
            shards,
            boundaries_arena: self.boundaries.into_boxed_slice(),
            labels_arena: self.labels.into_boxed_slice(),
            order_arena: self.order.into_boxed_slice(),
            labels_env: self.labels_env,
            label_term_bytes,
        }
    }

    fn spec_for<'a>(&mut self, metric: Term<'a>) -> Result<MetricSpec, StorageError> {
        let struct_name: Atom = metric
            .map_get(rustler::types::atom::__struct__())
            .map_err(|_| StorageError::BadArgument("metric must be a struct"))?
            .decode()
            .map_err(|_| StorageError::BadArgument("__struct__ must be an atom"))?;

        if struct_name == atoms::metric_counter() {
            Ok(MetricSpec::Counter)
        } else if struct_name == atoms::metric_sum() {
            Ok(MetricSpec::Sum)
        } else if struct_name == atoms::metric_last_value() {
            Ok(MetricSpec::LastValue)
        } else if struct_name == atoms::metric_distribution() {
            // Preserve integer boundaries: f64 loses precision above 2^53.
            let boundaries = metric
                .map_get(atoms::peep_bucket_boundaries())
                .map_err(|_| {
                    StorageError::BadArgument(
                        "Distribution metric must carry :peep_bucket_boundaries",
                    )
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

            let labels: Vec<Term> = metric
                .map_get(atoms::peep_bucket_labels())
                .map_err(|_| {
                    StorageError::BadArgument("Distribution metric must carry :peep_bucket_labels")
                })?
                .decode()
                .map_err(|_| StorageError::BadArgument(":peep_bucket_labels must be a list"))?;

            if labels.len() != boundaries.len().saturating_add(1) {
                return Err(StorageError::BadArgument(
                    ":peep_bucket_labels must have one more entry than :peep_bucket_boundaries",
                ));
            }

            Ok(MetricSpec::Distribution {
                boundaries: self.intern_boundaries(&boundaries)?,
                labels: self.store_labels(metric.get_env(), &labels)?,
            })
        } else {
            Err(StorageError::BadArgument("unrecognized metric struct"))
        }
    }

    /// Presort bucket keys once, rather than during each series' map construction.
    fn store_labels<'a>(
        &mut self,
        env: Env<'a>,
        labels: &[Term<'a>],
    ) -> Result<Span, StorageError> {
        let mut keys: Vec<(Term<'a>, u32)> = labels
            .iter()
            .enumerate()
            .map(|(bucket, label)| (*label, bucket as u32))
            .collect();

        keys.push((atoms::sum().encode(env), SUM_SLOT));
        keys.sort_by_key(|(key, _)| *key);

        // Duplicate labels, including `:sum`, would make scrape maps fail to build.
        if keys
            .iter()
            .zip(keys.iter().skip(1))
            .any(|(a, b)| a.0 == b.0)
        {
            return Err(StorageError::BadArgument(
                ":peep_bucket_labels must not repeat a bucket key",
            ));
        }

        let offset = self.labels.len();
        for (key, bucket) in &keys {
            let stored = self.labels_env.store(key.as_c_arg());
            self.labels.push(stored);
            self.order.push(*bucket);
        }

        Ok(Span {
            offset,
            len: keys.len(),
        })
    }

    fn intern_boundaries(&mut self, boundaries: &[Measurement]) -> Result<Span, StorageError> {
        // Duplicate boundaries create buckets no measurement can reach.
        if !boundaries.is_sorted_by(|a, b| a.cmp_exact(*b).is_lt()) {
            return Err(StorageError::UnsortedBoundaries);
        }

        for span in self.interned.iter().copied() {
            if span.of(&self.boundaries)? == boundaries {
                return Ok(span);
            }
        }

        let span = Span {
            offset: self.boundaries.len(),
            len: boundaries.len(),
        };
        self.boundaries.extend_from_slice(boundaries);
        self.interned.push(span);
        Ok(span)
    }
}

///////////////////////////////////////////////////////////////////////////////
//                               insert_metrics                              //
///////////////////////////////////////////////////////////////////////////////

/// Per-batch tag-index cache; higher indices fall back to interning per sample.
const TAG_CACHE: usize = 8;

/// On contention, return a shard hint without modifying any samples.
#[rustler::nif]
fn nif_insert_metrics<'a>(
    storage: &Storage,
    tag_results: Term<'a>,
    batch: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let registered = storage
        .registered
        .get()
        .ok_or(StorageError::NotRegistered)?;
    let shard_id = thread_shard();
    let lock = registered.shard(shard_id).ok_or(StorageError::NoShards)?;
    let env = tag_results.get_env();
    let Some(mut shard) = lock.try_write() else {
        return Ok((atoms::contended(), shard_id).encode(env));
    };

    Ok(insert_batch(registered, &mut shard, tag_results, batch)?.encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn nif_insert_metrics_dirty(
    storage: &Storage,
    shard_id: usize,
    tag_results: Term,
    batch: Term,
) -> Result<Atom, rustler::Error> {
    let registered = storage
        .registered
        .get()
        .ok_or(StorageError::NotRegistered)?;
    let mut shard = registered
        .shard(shard_id)
        .ok_or(StorageError::NoShards)?
        .write();
    insert_batch(registered, &mut shard, tag_results, batch)
}

#[inline]
fn insert_batch(
    registered: &RegisteredMetrics,
    shard: &mut Shard,
    tag_results: Term,
    batch: Term,
) -> Result<Atom, rustler::Error> {
    let tag_terms = TupleElements::new(tag_results)
        .ok_or(StorageError::BadArgument("tag_results must be a tuple"))?;

    let batch = batch
        .into_list_iterator()
        .map_err(|_| StorageError::BadArgument("batch must be a list"))?;

    let Shard { tags, metrics } = shard;

    let mut ids = [UNINTERNED; TAG_CACHE];
    // Gauges in one event share a timestamp.
    let mut now = None;

    for item in batch {
        // Avoids `Vec<Term>` allocations by `rustler::types::tuple::get_tuple`
        let (id, _metric, value, tag_idx): (usize, Term, Term, usize) =
            item.decode().map_err(|_| {
                StorageError::BadArgument("batch items must be {id, metric, value, tag_idx} tuples")
            })?;

        store_one(
            registered,
            metrics,
            id,
            value,
            || match ids.get(tag_idx).copied() {
                Some(cached) if cached != UNINTERNED => Ok(cached),
                _ => {
                    let term = tag_terms
                        .get(tag_idx)
                        .ok_or(StorageError::BadTagIndex(tag_idx))?;
                    let resolved = tags.intern(term)?;

                    if let Some(slot) = ids.get_mut(tag_idx) {
                        *slot = resolved;
                    }

                    Ok(resolved)
                }
            },
            &mut now,
        )?;
    }

    Ok(rustler::types::atom::ok())
}

#[inline]
fn store_one(
    registered: &RegisteredMetrics,
    metrics: &mut [MetricData],
    id: usize,
    value: Term,
    resolve_tags: impl FnOnce() -> Result<TagId, StorageError>,
    now: &mut Option<i64>,
) -> Result<(), StorageError> {
    match metrics.get_mut(id) {
        Some(MetricData::Counter(map)) => {
            let tag_id = resolve_tags()?;
            let total = map.entry(tag_id).or_insert(0);
            *total = total.wrapping_add(1);
            Ok(())
        }
        Some(MetricData::Sum(map)) => {
            let delta: i64 = value
                .decode()
                .map_err(|_| StorageError::BadMeasurement("sum value must be an integer"))?;

            let tag_id = resolve_tags()?;
            let total = map.entry(tag_id).or_insert(0);
            *total = total.wrapping_add(delta);
            Ok(())
        }
        Some(MetricData::LastValue(map)) => {
            let measurement = Measurement::decode(value)?;
            let tag_id = resolve_tags()?;
            let sample = (*now.get_or_insert_with(monotonic_time_ns), measurement);

            match map.entry(tag_id) {
                Entry::Occupied(entry) => {
                    let current = entry.into_mut();

                    if newer(sample, *current) {
                        *current = sample;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(sample);
                }
            }

            Ok(())
        }
        Some(MetricData::Distribution { boundaries, map }) => {
            let boundaries = boundaries.of(&registered.boundaries_arena)?;
            let measurement = Measurement::decode(value)?;
            // Validate before resolving tags or creating a series.
            let delta = measurement.rounded()?;
            let tag_id = resolve_tags()?;

            map.entry(tag_id)
                .or_insert_with(|| Buckets::new(boundaries.len()))
                .record(boundaries, measurement, delta)?;

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

/// Key/value storage plus hashbrown's control byte, excluding spare capacity.
const fn entry_bytes<K, V>() -> usize {
    size_of::<(K, V)>() + 1
}

#[rustler::nif(schedule = "DirtyCpu")]
fn storage_size(storage: &Storage) -> StorageSize {
    let Some(registered) = storage.registered.get() else {
        return StorageSize { size: 0, memory: 0 };
    };

    // The registry's label environment, plus one per shard.
    let mut memory = size_of::<RegisteredMetrics>()
        + (registered.shards.len() + 1) * ENV_OVERHEAD_BYTES
        + registered.metrics.len() * size_of::<MetricSpec>()
        + registered.boundaries_arena.len() * size_of::<Measurement>()
        + registered.labels_arena.len() * size_of::<NIF_TERM>()
        + registered.label_term_bytes
        + registered.order_arena.len() * size_of::<u32>()
        + registered.shards.len() * size_of::<CachePadded<RwLock<Shard>>>();

    let mut size = 0;

    // Estimate live content, not allocated capacity, so new series increase it.
    for shard in registered.shards.iter() {
        let shard = shard.0.read();

        memory += shard.tags.index.len() * entry_bytes::<TagsKey, TagId>()
            + shard.tags.keys.len() * size_of::<TagsKey>()
            + shard.tags.term_bytes()
            + shard.metrics.len() * size_of::<MetricData>();

        for data in &shard.metrics {
            let (entries, heap) = match data {
                MetricData::Counter(map) | MetricData::Sum(map) => {
                    (map.len(), map.len() * entry_bytes::<TagId, i64>())
                }
                MetricData::LastValue(map) => {
                    (map.len(), map.len() * entry_bytes::<TagId, Sample>())
                }
                // Bucket arrays are fixed at creation; recording never resizes them.
                MetricData::Distribution { boundaries, map } => (
                    map.len(),
                    map.len()
                        * (entry_bytes::<TagId, Buckets>()
                            + boundaries.len.saturating_add(1) * size_of::<u64>()),
                ),
            };

            size += entries;
            memory += heap;
        }
    }

    StorageSize { size, memory }
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

    // Check tuple shape and metric kinds, not identity: retaining the original
    // metric terms would also retain their functions.
    if metric_terms.len() != registered.metrics.len() {
        return Err(StorageError::MetricsLenMismatch {
            got: metric_terms.len(),
            want: registered.metrics.len(),
        }
        .into());
    }

    for (metric_id, (metric_term, spec)) in metric_terms.iter().zip(&registered.metrics).enumerate()
    {
        if !spec.describes(*metric_term) {
            return Err(StorageError::MetricKindMismatch { id: metric_id }.into());
        }
    }

    let mut caches: Vec<TagCache> = registered
        .shards
        .iter()
        .map(|_| TagCache::default())
        .collect();

    let mut outer_keys = Vec::new();
    let mut outer_vals = Vec::new();

    for (metric_id, (metric_term, spec)) in metric_terms.iter().zip(&registered.metrics).enumerate()
    {
        let (len, inner) = match spec {
            MetricSpec::Counter | MetricSpec::Sum => {
                encode_counter_map(env, registered, &mut caches, metric_id)?
            }
            MetricSpec::LastValue => {
                encode_last_value_map(env, registered, &mut caches, metric_id)?
            }
            MetricSpec::Distribution { labels, .. } => {
                encode_distribution_map(env, registered, &mut caches, metric_id, *labels)?
            }
        };

        if len > 0 {
            outer_keys.push(metric_term.as_c_arg());
            outer_vals.push(inner.as_c_arg());
        }
    }

    // SAFETY: metric arguments and encoded maps belong to this caller's env.
    let map = unsafe {
        map_from_raw_arrays(
            env,
            &outer_keys,
            &outer_vals,
            "duplicate metric definitions",
        )
    }?;

    Ok(map)
}

/// Copies belong to one scrape's caller environment and are reused across metrics.
/// Sync the generation under the shard lock before reusing IDs after pruning.
#[derive(Default)]
struct TagCache {
    generation: u64,
    keys: Vec<Option<TagsKey>>,
}

impl TagCache {
    fn sync(&mut self, tags: &TagTable) {
        if self.generation != tags.generation {
            self.generation = tags.generation;
            self.keys.clear();
        }
    }

    fn get(&self, id: usize) -> Option<TagsKey> {
        self.keys.get(id).copied().flatten()
    }

    fn remember(&mut self, tags: &TagTable, id: usize, key: TagsKey) -> Result<(), StorageError> {
        // Grow only when this shard supplies a key outside the cached prefix.
        if id >= self.keys.len() {
            self.keys.resize(tags.keys.len(), None);
        }

        let slot = self
            .keys
            .get_mut(id)
            .ok_or(StorageError::InvariantViolation(
                "tag cache does not cover tag id",
            ))?;

        *slot = Some(key);
        Ok(())
    }
}

/// Copy shard-owned keys into the caller's environment before releasing the lock.
fn merge_metric<V, A>(
    env: Env,
    registered: &RegisteredMetrics,
    caches: &mut [TagCache],
    metric_id: usize,
    project: impl Fn(&MetricData) -> Option<&IdMap<V>>,
    init: impl Fn(&V) -> A,
    merge: impl Fn(&mut A, &V) -> Result<(), StorageError>,
) -> Result<HashMap<TagsKey, A, TermHashBuilder>, StorageError> {
    if caches.len() != registered.shards.len() {
        return Err(StorageError::InvariantViolation(
            "tag cache count does not match shards",
        ));
    }
    let mut combined: HashMap<TagsKey, A, TermHashBuilder> = HashMap::default();
    let cache_keys = registered.metrics.len() > 1;

    for (shard, cache) in registered.shards.iter().zip(caches) {
        let shard = shard.0.read();

        let data = shard
            .metrics
            .get(metric_id)
            .ok_or(StorageError::InvariantViolation(
                "shard is missing registered metric",
            ))?;
        let map = project(data).ok_or(StorageError::InvariantViolation(
            "shard metric kind does not match registry",
        ))?;

        cache.sync(&shard.tags);

        // Use the first nonempty shard as an initial size estimate.
        if combined.capacity() == 0 {
            combined.reserve(map.len());
        }

        for (id, cell) in map.iter() {
            let key = shard
                .tags
                .keys
                .get(*id as usize)
                .ok_or(StorageError::InvariantViolation(
                    "metric references missing tag id",
                ))?;
            let cached = if cache_keys {
                cache.get(*id as usize)
            } else {
                None
            };
            let lookup = cached.as_ref().unwrap_or(key);

            match combined
                .raw_entry_mut()
                .from_hash(lookup.hash, |seen| seen == lookup)
            {
                RawEntryMut::Occupied(mut entry) => {
                    // Canonical caller-owned keys make later metrics' comparisons cheap.
                    if cache_keys && cached.map(|key| key.term) != Some(entry.key().term) {
                        cache.remember(&shard.tags, *id as usize, *entry.key())?;
                    }
                    merge(entry.get_mut(), cell)?;
                }
                RawEntryMut::Vacant(entry) => {
                    let copy = cached.unwrap_or_else(|| shard.tags.env.copy_key(env, key));
                    if cache_keys && cached.is_none() {
                        cache.remember(&shard.tags, *id as usize, copy)?;
                    }
                    entry.insert(copy, init(cell));
                }
            }
        }
    }

    Ok(combined)
}

fn encode_merged<'a, A>(
    env: Env<'a>,
    combined: &HashMap<TagsKey, A, TermHashBuilder>,
    label: &'static str,
    mut encode: impl FnMut(Env<'a>, &A) -> Result<Term<'a>, StorageError>,
) -> Result<(usize, Term<'a>), StorageError> {
    let mut keys = Vec::with_capacity(combined.len());
    let mut vals = Vec::with_capacity(combined.len());

    for (key, value) in combined {
        keys.push(key.term);
        vals.push(encode(env, value)?.as_c_arg());
    }

    // SAFETY: merge_metric copied keys into env; values were encoded in env.
    let map = unsafe { map_from_raw_arrays(env, &keys, &vals, label) }?;

    Ok((combined.len(), map))
}

fn encode_counter_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    caches: &mut [TagCache],
    metric_id: usize,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_metric(
        env,
        registered,
        caches,
        metric_id,
        |data| match data {
            MetricData::Counter(map) | MetricData::Sum(map) => Some(map),
            _ => None,
        },
        |cell: &i64| *cell,
        |total, cell| {
            *total = total.wrapping_add(*cell);
            Ok(())
        },
    )?;

    encode_merged(env, &combined, "counter map", |env, total| {
        Ok(total.encode(env))
    })
}

fn encode_last_value_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    caches: &mut [TagCache],
    metric_id: usize,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_metric(
        env,
        registered,
        caches,
        metric_id,
        |data| match data {
            MetricData::LastValue(map) => Some(map),
            _ => None,
        },
        |cell: &Sample| *cell,
        |newest, cell| {
            if newer(*cell, *newest) {
                *newest = *cell;
            }
            Ok(())
        },
    )?;

    encode_merged(env, &combined, "last_value map", |env, (_, value)| {
        Ok(value.encode(env))
    })
}

fn encode_distribution_map<'a>(
    env: Env<'a>,
    registered: &RegisteredMetrics,
    caches: &mut [TagCache],
    metric_id: usize,
    labels: Span,
) -> Result<(usize, Term<'a>), StorageError> {
    let combined = merge_metric(
        env,
        registered,
        caches,
        metric_id,
        |data| match data {
            MetricData::Distribution { map, .. } => Some(map),
            _ => None,
        },
        |cell: &Buckets| (cell.counts.clone(), cell.sum),
        |(counts, sum), cell| {
            if counts.len() != cell.counts.len() {
                return Err(StorageError::InvariantViolation(
                    "merged bucket counts have different lengths",
                ));
            }
            for (total, count) in counts.iter_mut().zip(cell.counts.iter()) {
                *total = total.wrapping_add(*count);
            }
            *sum = sum.wrapping_add(cell.sum);
            Ok(())
        },
    )?;

    // Avoid copying labels for a metric the scrape will omit.
    if combined.is_empty() {
        return Ok((0, Term::map_new(env)));
    }

    // Reuse caller-environment labels across this metric's series.
    let keys: Vec<NIF_TERM> = labels
        .of(&registered.labels_arena)?
        .iter()
        .map(|label| registered.labels_env.copy_out(env, *label).as_c_arg())
        .collect();
    let order = labels.of(&registered.order_arena)?;

    let mut vals: Vec<NIF_TERM> = Vec::with_capacity(labels.len);

    encode_merged(env, &combined, "distribution map", |env, (counts, sum)| {
        if labels.len.checked_sub(1) != Some(counts.len()) {
            return Err(StorageError::InvariantViolation(
                "bucket count does not match labels",
            ));
        }
        vals.clear();
        for &bucket in order {
            let value = if bucket == SUM_SLOT {
                sum.encode(env)
            } else {
                counts
                    .get(bucket as usize)
                    .ok_or(StorageError::InvariantViolation(
                        "bucket label references missing count",
                    ))?
                    .encode(env)
            };
            vals.push(value.as_c_arg());
        }

        // SAFETY: labels were copied into env and all values encoded there.
        // Clearing vals reuses only array storage, not the caller-owned terms.
        unsafe { map_from_raw_arrays(env, &keys, &vals, "distribution bucket map") }
    })
}

///////////////////////////////////////////////////////////////////////////////
//                                 prune_tags                                //
///////////////////////////////////////////////////////////////////////////////

#[rustler::nif(schedule = "DirtyCpu")]
fn prune_tags(storage: &Storage, patterns: Term) -> Result<Atom, rustler::Error> {
    let Some(registered) = storage.registered.get() else {
        return Ok(rustler::types::atom::ok());
    };

    let patterns: Vec<Term> = patterns
        .decode()
        .map_err(|_| StorageError::BadArgument("patterns must be a list"))?;

    if patterns.iter().any(|pattern| !pattern.is_map()) {
        return Err(StorageError::BadTagsMap.into());
    }

    // Avoid rebuilding every tags table when nothing can match.
    if patterns.is_empty() {
        return Ok(rustler::types::atom::ok());
    }

    for shard in registered.shards.iter() {
        prune_shard(&mut shard.0.write(), &patterns)?;
    }

    Ok(rustler::types::atom::ok())
}

/// Copy patterns in to match tags without copying every key to the caller.
/// Rebuild even without matches: environments have no per-term free, including
/// for the pattern copies. The write lock excludes inserts during renumbering.
fn prune_shard(shard: &mut Shard, patterns: &[Term]) -> Result<(), StorageError> {
    let Shard { tags, metrics } = shard;
    // Validate every metric before mutating the environment or any ID mapping.
    for data in metrics.iter() {
        match data {
            MetricData::Counter(map) | MetricData::Sum(map) => {
                validate_tag_ids(map, tags.keys.len())?
            }
            MetricData::LastValue(map) => validate_tag_ids(map, tags.keys.len())?,
            MetricData::Distribution { map, .. } => validate_tag_ids(map, tags.keys.len())?,
        }
    }

    let TagTable {
        env,
        index,
        keys,
        term_bytes,
        generation,
    } = tags;

    let patterns: Vec<NIF_TERM> = patterns
        .iter()
        .map(|pattern| env.store(pattern.as_c_arg()))
        .collect();

    let mut remap: Vec<Option<TagId>> = Vec::with_capacity(keys.len());
    let mut fresh_env = TagsEnv::new();
    let mut fresh_keys: Vec<TagsKey> = Vec::with_capacity(keys.len());

    for key in keys.iter() {
        if env.matches_any(&patterns, key.term) {
            remap.push(None);
        } else {
            remap.push(Some(fresh_keys.len() as TagId));
            fresh_keys.push(TagsKey {
                hash: key.hash,
                term: fresh_env.store(key.term),
            });
        }
    }

    let mut fresh_index: HashMap<TagsKey, TagId, TermHashBuilder> =
        HashMap::with_capacity_and_hasher(fresh_keys.len(), TermHashBuilder::default());

    for (id, key) in fresh_keys.iter().enumerate() {
        fresh_index.insert(*key, id as TagId);
    }

    let renumbered = fresh_keys.len() != keys.len();

    if renumbered {
        for data in metrics.iter_mut() {
            match data {
                MetricData::Counter(map) | MetricData::Sum(map) => remap_map(map, &remap)?,
                MetricData::LastValue(map) => remap_map(map, &remap)?,
                MetricData::Distribution { map, .. } => remap_map(map, &remap)?,
            }
        }

        // A scrape mid-flight must discard its cached copies of moved ids.
        *generation = generation.wrapping_add(1);
    }

    *env = fresh_env;
    *index = fresh_index;
    *keys = fresh_keys;
    if renumbered {
        term_bytes.store(
            if keys.is_empty() { 0 } else { usize::MAX },
            Ordering::Relaxed,
        );
    }
    Ok(())
}

fn validate_tag_ids<V>(map: &IdMap<V>, tag_count: usize) -> Result<(), StorageError> {
    if map.keys().any(|id| *id as usize >= tag_count) {
        return Err(StorageError::InvariantViolation(
            "metric references missing tag id",
        ));
    }
    Ok(())
}

fn remap_map<V>(map: &mut IdMap<V>, remap: &[Option<TagId>]) -> Result<(), StorageError> {
    // Validate before draining: an invalid id must leave all entries intact.
    validate_tag_ids(map, remap.len())?;
    let mut kept: IdMap<V> = HashMap::with_capacity_and_hasher(map.len(), IdHashBuilder::default());

    for (id, value) in map.drain() {
        let mapped = remap
            .get(id as usize)
            .ok_or(StorageError::InvariantViolation(
                "tag remap does not cover tag id",
            ))?;
        if let Some(new_id) = mapped {
            kept.insert(*new_id, value);
        }
    }

    *map = kept;
    Ok(())
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

    // TagsKey uses write_u64; keep the fallback non-panicking.
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type TermHashBuilder = BuildHasherDefault<TermPassthroughHasher>;

/// Spread dense tag IDs into the high bits hashbrown uses for control bytes.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(byte);
        }
    }

    fn write_u32(&mut self, value: u32) {
        self.0 = u64::from(value).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

type IdHashBuilder = BuildHasherDefault<IdHasher>;

/// Shard-owned terms may only be read under that shard's lock. `copy_key`
/// produces caller-environment terms valid for the remainder of the NIF call.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_reject_missing_and_overflowed_ranges() -> Result<(), StorageError> {
        let arena = [1_u8, 2, 3];
        assert_eq!(Span { offset: 1, len: 2 }.of(&arena)?, &[2, 3]);
        assert!(Span { offset: 3, len: 0 }.of(&arena)?.is_empty());

        for span in [
            Span { offset: 2, len: 2 },
            Span { offset: 4, len: 0 },
            Span {
                offset: usize::MAX,
                len: 1,
            },
        ] {
            assert!(matches!(
                span.of(&arena),
                Err(StorageError::InvariantViolation(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn missing_bucket_leaves_counts_and_sum_unchanged() {
        let mut buckets = Buckets {
            counts: vec![4].into_boxed_slice(),
            sum: 17,
        };

        assert!(matches!(
            buckets.record(&[Measurement::Int(10)], Measurement::Int(10), 10),
            Err(StorageError::InvariantViolation(_))
        ));
        assert_eq!(buckets.counts.as_ref(), &[4]);
        assert_eq!(buckets.sum, 17);
    }

    #[test]
    fn invalid_remap_preserves_entries() -> Result<(), StorageError> {
        let original: IdMap<_> = [(0, 10), (3, 30)].into_iter().collect();
        let mut map = original.clone();

        assert!(matches!(
            remap_map(&mut map, &[None, Some(0)]),
            Err(StorageError::InvariantViolation(_))
        ));
        assert_eq!(map, original);

        remap_map(&mut map, &[None, None, None, Some(0)])?;
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&0), Some(&30));
        Ok(())
    }
}
