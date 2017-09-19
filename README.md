# H2O Utility #

A GUI-based and optionally headless tool to select times series values from a HydroServer, write the data to CSV files, and upload them to their respective HydroShare resources. The VisualUpdater must be used to intially select which HydroServer, HydroShare, and ODM Series should be used.

###### Requirements ######

These tools are written for Python 2.7.X

To download, install, and run this project, execute the following commands:

> Note: To use a virtual environment with this project, create and activate your virtual environment before running these commands.

```sh
git clone https://github.com/UCHIC/h2outility.git
cd h2outility
python -m pip install -r ./src/requirements.txt
python ./src/VisualUpdater.py
python ./src/SilentUpdater.py
```

***

#### Visual Updater Utility ####

VisualUpdater.py is used to create a series of "rules" used to collect HydroServer time series values, create CSV files, and upload these to HydroShare resources. To run, simply run the following command (arguments listed below are optional):

```sh
python ./src/VisualUpdater.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_File.txt`; stderr output is not redirected to log file|

###### Running the Headless-mode SilentUpdater #####

After you have established the HydroServer-HydroShare rules for your ODM time series, you can run the SilentUpdater.py script to update these without needing to use a GUI. This is useful for scheduling run times to keep your HydroShare resources up to date.

To run the Silent Updater, simply run the following command (arguments listed below are optional):
```sh
python ./src/SilentUpdater.py
```

| Argument | Description |
| --- | --- |
|`--verbose`|Prints out extra output (lots and lots of extra output)|
|`--debug`|Creates or overwrites log file `Log_{script}_File.txt`; stderr output is not redirected to log file|

