#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import sys

from airbyte_cdk.entrypoint import launch
from source_parsersvc_connector import SourceParsersVc
if __name__ == "__main__":
    source = SourceParsersVc()
    launch(source, sys.argv[1:])
