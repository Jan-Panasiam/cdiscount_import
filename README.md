# cdiscount_import

Accessing plentymarket Api and restructuring the data into an cdiscount uploadfile while
doing error checks and doing an extra list for it.

## Installation

In order to use the application you require git, a running python3 installation and poetry.
You can install poetry using the pip package manager.
Here is an example installation on a debian system:
```bash
# Install dependencies
sudo apt-get install python3-pip git
python3 -m pip install poetry

# Get the project
git clone https://github.com/Jan-Panasiam/webcam_panasiam_mail.git
cd cdiscount_import
# This will install all other required dependencies
poetry install

#This will set up the configuration folder and a empty config.ini file.
python3 -m cdiscount_import
# Test if the configuration was created
ls ~/.config/cdiscount_import/config.ini
```

##Configuration

Before you can successfully run the script, you have to configure your system. To do that you have to adjust the `config.ini` file located at the `.config/cdiscount_import` folder.
On Linux systems that folder is creatad within `/home/{USER}`.
There you have to give the following information:
```
[general]
teplate_path = (path to cdiscounts uploadfile template)

[plenty]
base_url = (url to access your plenty Api)
color_attribute_id = (id in plentymarket)
size_attribute_id =  (id in plentymarket)
size_property_id =  (id in plentymarket)
referrer_id = (cdiscount market id in plentymarket)
ean_barcode_id = 1  (id in plentymarket)
plenty:id = (plentymarket id)

[category_mapping]
(mapping of plenty categories to cdiscount categories)
```

## Usage

`cdiscoun_import` is a program that you can use to extract your data from the plentymarket Api and get a cdiscount_import file and a cdiscount_error file.
```bash
python3 -m cdiscount_import
```
