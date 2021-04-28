![WiM](wimlogo.png)


# StreamStats-Tools

A collection of tools for StreamStats data preparation and upload to AWS

### Prerequisites

ArcGIS

[Archydro](http://downloads.esri.com/archydro/archydro/setup/) - the version should correlate with your local ArcGIS version

[Amazon Web Services CLI](https://docs.aws.amazon.com/cli/latest/userguide/installing.html)

`Secrets.py` - you will need a `secrets.py` file placed within the `src` folder. It should be formatted:
```
accessKeyID = 'xxxxxxxxxxxxxxxxx'
accessKey = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
```

where the accessKeyID is the username and accessKey is the password from the wim_gis_user found in the WIM Keepass > StreamStats. Note that this file is in the .gitignore and should NEVER be pushed up to GitHub.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Building and testing

To test your edits and use the tool, open ArcCatalog or ArcMap and navigate to your local repo directory. Expand the "src" folder, and you should see a toolbox named "ss-tools.pyt". You can also run the python files separately, but there are often inconsistencies when run in ArcGIS.

You may need to restart your ArcGIS session to see your edits.

## Deployment
This project just needs to be zipped and updated in a few places. To do so, zip up the "src" folder and upload it on Google Drive [here](https://drive.google.com/drive/u/0/folders/1faN_-vHzYja89JdCSzQjyis6vSdqrmNd), as well as on vm103 at E:/WIM/Tools. 

Check to make sure any big changes are reflected in the StreamStats ArcToolbox tools confluence page, and let the GIS team know of any updates.

## Built With

* [Python Toolbox](http://desktop.arcgis.com/en/arcmap/10.3/analyze/creating-tools/a-quick-tour-of-python-toolboxes.htm)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on the process for submitting pull requests to us. Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details on adhering by the [USGS Code of Scientific Conduct](https://www2.usgs.gov/fsp/fsp_code_of_scientific_conduct.asp).

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](../../tags). 

Advance the version when adding features, fixing bugs or making minor enhancement. Follow semver principles. To add tag in git, type git tag v{major}.{minor}.{patch}. Example: git tag v2.0.5

To push tags to remote origin: `git push origin --tags`

*Note that your alias for the remote origin may differ.

## Authors

* **[Katrin Jacobsen](https://www.usgs.gov/staff-profiles/katrin-jacobsen)**  - *Lead Developer* - [USGS Web Informatics & Mapping](https://wim.usgs.gov/)
* **[Marty Smith](https://www.usgs.gov/staff-profiles/martyn-smith)**  - *Developer* - [USGS Web Informatics & Mapping](https://wim.usgs.gov/)

See also the list of [contributors](../../graphs/contributors) who participated in this project.

## License

This project is licensed under the Creative Commons CC0 1.0 Universal License - see the [LICENSE.md](LICENSE.md) file for details

## Suggested Citation
In the spirit of open source, please cite any re-use of the source code stored in this repository. Below is the suggested citation:

`This project contains code produced by the Web Informatics and Mapping (WIM) team at the United States Geological Survey (USGS). As a work of the United States Government, this project is in the public domain within the United States. https://wim.usgs.gov`

## Acknowledgements

## About WIM
* This project authored by the [USGS WIM team](https://wim.usgs.gov)
* WIM is a team of developers and technologists who build and manage tools, software, web services, and databases to support USGS science and other federal government cooperators.
* WIM is a part of the [Upper Midwest Water Science Center](https://www.usgs.gov/centers/wisconsin-water-science-center).
