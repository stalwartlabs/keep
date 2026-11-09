# Change Log

All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## [1.0.2] - Unreleased

## [1.0.1] - 2026-11-09

## Changed
- Arena memory release in the upkeep step starts at the first size class it can return to the system (8 KiB), instead of scanning the smaller classes first, which shortens each upkeep step.

## [1.0.0] - 2026-11-09

Initial release.
