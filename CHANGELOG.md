# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/USGS-WiM/StreamStats-Tools/tree/dev)

### Added 

- "MRB" to regions in config.json
- CHANGELOG.md

### Changed  

-

### Deprecated 

-

### Removed 

- 

### Fixed  

- 

### Security  

- 

## [v1.2.0](https://github.com/USGS-WiM/StreamStats-Tools/releases/tag/v1.2.0) - 2021-04-29

### Changed

- Change S3 credentials to use general user
- Enhance error messages

### Removed 

- AWS key inputs

## [v1.1.1](https://github.com/USGS-WiM/StreamStats-Tools/releases/tag/v1.1.1) - 2019-09-26

### Added 

- Added PR to state list
- Added config file w/ region list and necessary xml nodes for easy additions when needed
- Indicated need for square brackets for delineation pourpoint

### Changed  

- Moved around XML validation
- Changed "Select input state/region folder" wording
- Set default temp workspace to E:/staging/data in update to s3 tool if it exists

### Removed 

- Removed "optional" from xml

## [v1.1.0](https://github.com/USGS-WiM/StreamStats-Tools/releases/tag/v1.1.0) - 2019-04-09

### Added

- Added tools to pull/push to S3
- Added script to remove unnecessary data
- Added workarounds for some states
