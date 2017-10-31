Application to update Google Drive files with SGID data.

#### Install
1. `git clone` repository to your machine
1. Create or obtain secret files and add them to local repository
  - Use a service account for server based file update
  - Use application client token for user based file creation and update
1. Use shell of your choice and navigate to local repository directory
1. Run `python zip_loader.py -h` for a list of options
  - Most common usage is: ` python zip_loader.py "path to workspace containing features" --feature "your.full.featurename"`
