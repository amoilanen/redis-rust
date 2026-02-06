[![progress-banner](https://backend.codecrafters.io/progress/redis/65d6133d-6a7a-41d4-bd2f-6e09df044b02)](https://app.codecrafters.io/users/amoilanen?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

## ✨ Code Quality Improvements

This implementation has been thoroughly refactored and tested:

- **89 comprehensive tests** covering CLI, commands, storage, IO, RDB, and more
- **Well-organized module structure** with clear separation of concerns
- **Extensive documentation** with examples for all public APIs
- **Rust best practices** applied throughout the codebase
- **100% backward compatible** - all functionality preserved

See [REFACTORING_COMPLETE.md](./REFACTORING_COMPLETE.md) for detailed refactoring notes.

## Getting Started

### Passing the first stage

The entry point for your Redis implementation is in `src/main.rs`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git add .
git commit -m "pass 1st stage" # any msg
git push origin master
```

That's all!

## Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cargo (1.54)` installed locally
1. Run `./spawn_redis_server.sh` to run your Redis server, which is implemented
   in `src/main.rs`. This command compiles your Rust project, so it might be
   slow the first time you run it. Subsequent runs will be fast.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.

## Testing

Run all tests:
```sh
cargo test --lib
```

Run tests for a specific module:
```sh
cargo test --lib cli::tests
cargo test --lib commands_tests::tests
cargo test --lib storage_tests::tests
```
