#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


"""
constants.py includes a list of all known pokemon for config validation in source.py.
"""
TOPICS = ["data_team", "funds", "fundingRound_references", "fundingRound_partners",
          "fundingRounds", "news", "startups",
          "startup_references", "investments"]
TYPES = ['csv', 'json']
PARSERSVC_URL = "https://vcapi.parsers.me/v2/dump"
