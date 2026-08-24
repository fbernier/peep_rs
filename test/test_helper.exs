ExUnit.start()

# Drive Peep's shared storage suite (test/shared/storage_test.exs, shipped with
# the peep dependency) against the Rust NIF backend.
Application.put_env(:peep, :test_storages, [{Peep.Storage.RustNIF, []}])

peep = Mix.Project.deps_paths()[:peep]
Code.require_file(Path.join(peep, "test/shared/test_helpers.ex"))
Code.require_file(Path.join(peep, "test/shared/storage_test.exs"))
