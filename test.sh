#!/bin/bash
pip install pip airbyte-cdk~=0.2 gcsfs==2022.7.1 genson==1.2.2 google-cloud-storage==2.5.0 pandas==1.4.3 paramiko==2.11.0 s3fs==2022.7.1 boto3==1.21.21 smart-open lxml==4.9.1 html5lib==1.1 beautifulsoup4==4.11.1 pyarrow==9.0.0 xlrd==2.0.1 openpyxl==3.0.10 pyxlsb==1.0.9

python main.py check --config source_config.json
