import re
from snowflake.snowpark.functions import col, call_udf
from snowflake.snowpark.session import Session
from snowflake.snowpark.column import Column
from typing import Dict, Any

from snowflake.snowpark import DataFrame, DataFrameReader, DataFrameWriter, Row


IBAN_SPECIFICATIONS: Dict[str, Dict[str, Any]] = {
  "AD": {
    "country": "AD",
    "in_sepa_zone": "true",
    "bban_spec": "4!n4!n12!c",
    "bban_length": 20,
    "iban_spec": "AD2!n4!n4!n12!c",
    "iban_length": 24,
    "positions": {
      "account_code": [
        8,
        20
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "AE": {
    "country": "AE",
    "in_sepa_zone": "false",
    "bban_spec": "3!n16!n",
    "bban_length": 19,
    "iban_spec": "AE2!n3!n16!n",
    "iban_length": 23,
    "positions": {
      "account_code": [
        3,
        19
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "AL": {
    "country": "AL",
    "in_sepa_zone": "false",
    "bban_spec": "8!n16!c",
    "bban_length": 24,
    "iban_spec": "AL2!n8!n16!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        8,
        24
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        8
      ]
    }
  },
  "AT": {
    "country": "AT",
    "in_sepa_zone": "true",
    "bban_spec": "5!n11!n",
    "bban_length": 16,
    "iban_spec": "AT2!n5!n11!n",
    "iban_length": 20,
    "positions": {
      "account_code": [
        5,
        16
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "AZ": {
    "country": "AZ",
    "in_sepa_zone": "false",
    "bban_spec": "4!a20!c",
    "bban_length": 24,
    "iban_spec": "AZ2!n4!a20!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "BA": {
    "country": "BA",
    "in_sepa_zone": "false",
    "bban_spec": "3!n3!n8!n2!n",
    "bban_length": 16,
    "iban_spec": "BA2!n3!n3!n8!n2!n",
    "iban_length": 20,
    "positions": {
      
      "account_code": [
        6,
        16
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        6
      ]
    }
  },
  "BE": {
    "country": "BE",
    "in_sepa_zone": "true",
    "bban_spec": "3!n7!n2!n",
    "bban_length": 12,
    "iban_spec": "BE2!n3!n7!n2!n",
    "iban_length": 16,
    "positions": {
      "account_code": [
        3,
        12
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "BG": {
    "country": "BG",
    "in_sepa_zone": "true",
    "bban_spec": "4!a4!n2!n8!c",
    "bban_length": 18,
    "iban_spec": "BG2!n4!a4!n2!n8!c",
    "iban_length": 22,
    "positions": {
      "account_code": [
        8,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "BH": {
    "country": "BH",
    "in_sepa_zone": "false",
    "bban_spec": "4!a14!c",
    "bban_length": 18,
    "iban_spec": "BH2!n4!a14!c",
    "iban_length": 22,
    "positions": {
      "account_code": [
        4,
        18
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "BI": {
    "country": "BI",
    "in_sepa_zone": "false",
    "bban_spec": "5!n5!n11!n2!n",
    "bban_length": 23,
    "iban_spec": "BI2!n5!n5!n11!n2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        10,
        23
      ],
      "bank_code": [
        0,
        5
      ],
      "branch_code": [
        5,
        10
      ]
    }
  },
  "BR": {
    "country": "BR",
    "in_sepa_zone": "false",
    "bban_spec": "8!n5!n10!n1!a1!c",
    "bban_length": 25,
    "iban_spec": "BR2!n8!n5!n10!n1!a1!c",
    "iban_length": 29,
    "positions": {
      "account_code": [
        13,
        25
      ],
      "bank_code": [
        0,
        8
      ],
      "branch_code": [
        8,
        13
      ]
    }
  },
  "BY": {
    "country": "BY",
    "in_sepa_zone": "false",
    "bban_spec": "4!c4!n16!c",
    "bban_length": 24,
    "iban_spec": "BY2!n4!c4!n16!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "CH": {
    "country": "CH",
    "in_sepa_zone": "true",
    "bban_spec": "5!n12!c",
    "bban_length": 17,
    "iban_spec": "CH2!n5!n12!c",
    "iban_length": 21,
    "positions": {
      "account_code": [
        5,
        17
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "CR": {
    "country": "CR",
    "in_sepa_zone": "false",
    "bban_spec": "4!n14!n",
    "bban_length": 18,
    "iban_spec": "CR2!n4!n14!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        4,
        18
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "CY": {
    "country": "CY",
    "in_sepa_zone": "true",
    "bban_spec": "3!n5!n16!c",
    "bban_length": 24,
    "iban_spec": "CY2!n3!n5!n16!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        8,
        24
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        8
      ]
    }
  },
  "CZ": {
    "country": "CZ",
    "in_sepa_zone": "true",
    "bban_spec": "4!n6!n10!n",
    "bban_length": 20,
    "iban_spec": "CZ2!n4!n6!n10!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        4,
        20
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "DE": {
    "country": "DE",
    "in_sepa_zone": "true",
    "bban_spec": "8!n10!n",
    "bban_length": 18,
    "iban_spec": "DE2!n8!n10!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        8,
        18
      ],
      "bank_code": [
        0,
        8
      ]
    }
  },
  "DJ": {
    "country": "DJ",
    "in_sepa_zone": "false",
    "bban_spec": "5!n5!n11!n2!n",
    "bban_length": 23,
    "iban_spec": "DJ2!n5!n5!n11!n2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        10,
        23
      ],
      "bank_code": [
        0,
        5
      ],
      "branch_code": [
        5,
        10
      ]
    }
  },
  "DK": {
    "country": "DK",
    "in_sepa_zone": "true",
    "bban_spec": "4!n9!n1!n",
    "bban_length": 14,
    "iban_spec": "DK2!n4!n9!n1!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        4,
        14
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "DO": {
    "country": "DO",
    "in_sepa_zone": "false",
    "bban_spec": "4!c20!n",
    "bban_length": 24,
    "iban_spec": "DO2!n4!c20!n",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "EE": {
    "country": "EE",
    "in_sepa_zone": "true",
    "bban_spec": "2!n2!n11!n1!n",
    "bban_length": 16,
    "iban_spec": "EE2!n2!n2!n11!n1!n",
    "iban_length": 20,
    "positions": {
      "account_code": [
        2,
        16
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "EG": {
    "country": "EG",
    "in_sepa_zone": "false",
    "bban_spec": "4!n4!n17!n",
    "bban_length": 25,
    "iban_spec": "EG2!n4!n4!n17!n",
    "iban_length": 29,
    "positions": {
      "account_code": [
        8,
        25
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "ES": {
    "country": "ES",
    "in_sepa_zone": "true",
    "bban_spec": "4!n4!n1!n1!n10!n",
    "bban_length": 20,
    "iban_spec": "ES2!n4!n4!n1!n1!n10!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        8,
        20
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "FI": {
    "country": "FI",
    "in_sepa_zone": "true",
    "bban_spec": "3!n11!n",
    "bban_length": 14,
    "iban_spec": "FI2!n3!n11!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        3,
        14
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "AX": {
    "country": "AX",
    "in_sepa_zone": "true",
    "bban_spec": "3!n11!n",
    "bban_length": 14,
    "iban_spec": "FI2!n3!n11!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        3,
        14
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "FK": {
    "country": "FK",
    "in_sepa_zone": "false",
    "bban_spec": "2!a12!n",
    "bban_length": 14,
    "iban_spec": "FK2!n2!a12!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        2,
        14
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "FO": {
    "country": "FO",
    "in_sepa_zone": "false",
    "bban_spec": "4!n9!n1!n",
    "bban_length": 14,
    "iban_spec": "FO2!n4!n9!n1!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        4,
        14
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "FR": {
    "country": "FR",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "GF": {
    "country": "GF",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "GP": {
    "country": "GP",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "MQ": {
    "country": "MQ",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "RE": {
    "country": "RE",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "PF": {
    "country": "PF",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "TF": {
    "country": "TF",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "YT": {
    "country": "YT",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "NC": {
    "country": "NC",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "BL": {
    "country": "BL",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "MF": {
    "country": "MF",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "PM": {
    "country": "PM",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "WF": {
    "country": "WF",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "FR2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        5,
        23
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "GB": {
    "country": "GB",
    "in_sepa_zone": "true",
    "bban_spec": "4!a6!n8!n",
    "bban_length": 18,
    "iban_spec": "GB2!n4!a6!n8!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        10,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        10
      ]
    }
  },
  "IM": {
    "country": "IM",
    "in_sepa_zone": "true",
    "bban_spec": "4!a6!n8!n",
    "bban_length": 18,
    "iban_spec": "GB2!n4!a6!n8!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        10,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        10
      ]
    }
  },
  "JE": {
    "country": "JE",
    "in_sepa_zone": "true",
    "bban_spec": "4!a6!n8!n",
    "bban_length": 18,
    "iban_spec": "GB2!n4!a6!n8!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        10,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        10
      ]
    }
  },
  "GG": {
    "country": "GG",
    "in_sepa_zone": "true",
    "bban_spec": "4!a6!n8!n",
    "bban_length": 18,
    "iban_spec": "GB2!n4!a6!n8!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        10,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        10
      ]
    }
  },
  "GE": {
    "country": "GE",
    "in_sepa_zone": "false",
    "bban_spec": "2!a16!n",
    "bban_length": 18,
    "iban_spec": "GE2!n2!a16!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        2,
        18
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "GI": {
    "country": "GI",
    "in_sepa_zone": "true",
    "bban_spec": "4!a15!c",
    "bban_length": 19,
    "iban_spec": "GI2!n4!a15!c",
    "iban_length": 23,
    "positions": {
      "account_code": [
        4,
        19
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "GL": {
    "country": "GL",
    "in_sepa_zone": "false",
    "bban_spec": "4!n9!n1!n",
    "bban_length": 14,
    "iban_spec": "GL2!n4!n9!n1!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        4,
        14
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "GR": {
    "country": "GR",
    "in_sepa_zone": "true",
    "bban_spec": "3!n4!n16!c",
    "bban_length": 23,
    "iban_spec": "GR2!n3!n4!n16!c",
    "iban_length": 27,
    "positions": {
      "account_code": [
        7,
        23
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        7
      ]
    }
  },
  "GT": {
    "country": "GT",
    "in_sepa_zone": "false",
    "bban_spec": "4!c20!c",
    "bban_length": 24,
    "iban_spec": "GT2!n4!c20!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "HR": {
    "country": "HR",
    "in_sepa_zone": "true",
    "bban_spec": "7!n10!n",
    "bban_length": 17,
    "iban_spec": "HR2!n7!n10!n",
    "iban_length": 21,
    "positions": {
      "account_code": [
        7,
        17
      ],
      "bank_code": [
        0,
        7
      ]
    }
  },
  "HU": {
    "country": "HU",
    "in_sepa_zone": "true",
    "bban_spec": "3!n4!n1!n15!n1!n",
    "bban_length": 24,
    "iban_spec": "HU2!n3!n4!n1!n15!n1!n",
    "iban_length": 28,
    "positions": {
      "account_code": [
        7,
        24
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        7
      ]
    }
  },
  "IE": {
    "country": "IE",
    "in_sepa_zone": "true",
    "bban_spec": "4!a6!n8!n",
    "bban_length": 18,
    "iban_spec": "IE2!n4!a6!n8!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        10,
        18
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        10
      ]
    }
  },
  "IL": {
    "country": "IL",
    "in_sepa_zone": "false",
    "bban_spec": "3!n3!n13!n",
    "bban_length": 19,
    "iban_spec": "IL2!n3!n3!n13!n",
    "iban_length": 23,
    "positions": {
      "account_code": [
        6,
        19
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        6
      ]
    }
  },
  "IQ": {
    "country": "IQ",
    "in_sepa_zone": "false",
    "bban_spec": "4!a3!n12!n",
    "bban_length": 19,
    "iban_spec": "IQ2!n4!a3!n12!n",
    "iban_length": 23,
    "positions": {
      "account_code": [
        7,
        19
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        7
      ]
    }
  },
  "IS": {
    "country": "IS",
    "in_sepa_zone": "false",
    "bban_spec": "4!n2!n6!n10!n",
    "bban_length": 22,
    "iban_spec": "IS2!n4!n2!n6!n10!n",
    "iban_length": 26,
    "positions": {
      "account_code": [
        4,
        22
      ],
      "bank_code": [
        0,
        2
      ],
      "branch_code": [
        2,
        4
      ]
    }
  },
  "IT": {
    "country": "IT",
    "in_sepa_zone": "true",
    "bban_spec": "1!a5!n5!n12!c",
    "bban_length": 23,
    "iban_spec": "IT2!n1!a5!n5!n12!c",
    "iban_length": 27,
    "positions": {
      "account_code": [
        11,
        23
      ],
      "bank_code": [
        1,
        6
      ],
      "branch_code": [
        6,
        11
      ]
    }
  },
  "JO": {
    "country": "JO",
    "in_sepa_zone": "false",
    "bban_spec": "4!a4!n18!c",
    "bban_length": 26,
    "iban_spec": "JO2!n4!a4!n18!c",
    "iban_length": 30,
    "positions": {
      "account_code": [
        8,
        26
      ],
      "bank_code": [
        4,
        8
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "KW": {
    "country": "KW",
    "in_sepa_zone": "false",
    "bban_spec": "4!a22!c",
    "bban_length": 26,
    "iban_spec": "KW2!n4!a22!c",
    "iban_length": 30,
    "positions": {
      "account_code": [
        4,
        26
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "KZ": {
    "country": "KZ",
    "in_sepa_zone": "false",
    "bban_spec": "3!n13!c",
    "bban_length": 16,
    "iban_spec": "KZ2!n3!n13!c",
    "iban_length": 20,
    "positions": {
      "account_code": [
        3,
        16
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "LB": {
    "country": "LB",
    "in_sepa_zone": "false",
    "bban_spec": "4!n20!c",
    "bban_length": 24,
    "iban_spec": "LB2!n4!n20!c",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "LC": {
    "country": "LC",
    "in_sepa_zone": "false",
    "bban_spec": "4!a24!c",
    "bban_length": 28,
    "iban_spec": "LC2!n4!a24!c",
    "iban_length": 32,
    "positions": {
      "account_code": [
        4,
        28
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "LI": {
    "country": "LI",
    "in_sepa_zone": "true",
    "bban_spec": "5!n12!c",
    "bban_length": 17,
    "iban_spec": "LI2!n5!n12!c",
    "iban_length": 21,
    "positions": {
      "account_code": [
        5,
        17
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "LT": {
    "country": "LT",
    "in_sepa_zone": "true",
    "bban_spec": "5!n11!n",
    "bban_length": 16,
    "iban_spec": "LT2!n5!n11!n",
    "iban_length": 20,
    "positions": {
      "account_code": [
        5,
        16
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "LU": {
    "country": "LU",
    "in_sepa_zone": "true",
    "bban_spec": "3!n13!c",
    "bban_length": 16,
    "iban_spec": "LU2!n3!n13!c",
    "iban_length": 20,
    "positions": {
      "account_code": [
        3,
        16
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "LV": {
    "country": "LV",
    "in_sepa_zone": "true",
    "bban_spec": "4!a13!c",
    "bban_length": 17,
    "iban_spec": "LV2!n4!a13!c",
    "iban_length": 21,
    "positions": {
      "account_code": [
        4,
        17
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "LY": {
    "country": "LY",
    "in_sepa_zone": "false",
    "bban_spec": "3!n3!n15!n",
    "bban_length": 21,
    "iban_spec": "LY2!n3!n3!n15!n",
    "iban_length": 25,
    "positions": {
      "account_code": [
        6,
        21
      ],
      "bank_code": [
        0,
        3
      ],
      "branch_code": [
        3,
        6
      ]
    }
  },
  "MC": {
    "country": "MC",
    "in_sepa_zone": "true",
    "bban_spec": "5!n5!n11!c2!n",
    "bban_length": 23,
    "iban_spec": "MC2!n5!n5!n11!c2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        10,
        23
      ],
      "bank_code": [
        0,
        5
      ],
      "branch_code": [
        5,
        10
      ]
    }
  },
  "MD": {
    "country": "MD",
    "in_sepa_zone": "false",
    "bban_spec": "2!c18!c",
    "bban_length": 20,
    "iban_spec": "MD2!n2!c18!c",
    "iban_length": 24,
    "positions": {
      "account_code": [
        2,
        20
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "ME": {
    "country": "ME",
    "in_sepa_zone": "false",
    "bban_spec": "3!n13!n2!n",
    "bban_length": 18,
    "iban_spec": "ME2!n3!n13!n2!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        3,
        18
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "MK": {
    "country": "MK",
    "in_sepa_zone": "false",
    "bban_spec": "3!n10!c2!n",
    "bban_length": 15,
    "iban_spec": "MK2!n3!n10!c2!n",
    "iban_length": 19,
    "positions": {
      "account_code": [
        3,
        15
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "MN": {
    "country": "MN",
    "in_sepa_zone": "false",
    "bban_spec": "4!n12!n",
    "bban_length": 16,
    "iban_spec": "MN2!n4!n12!n",
    "iban_length": 20,
    "positions": {
      "account_code": [
        4,
        16
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "MR": {
    "country": "MR",
    "in_sepa_zone": "false",
    "bban_spec": "5!n5!n11!n2!n",
    "bban_length": 23,
    "iban_spec": "MR2!n5!n5!n11!n2!n",
    "iban_length": 27,
    "positions": {
      "account_code": [
        10,
        23
      ],
      "bank_code": [
        0,
        5
      ],
      "branch_code": [
        5,
        10
      ]
    }
  },
  "MT": {
    "country": "MT",
    "in_sepa_zone": "true",
    "bban_spec": "4!a5!n18!c",
    "bban_length": 27,
    "iban_spec": "MT2!n4!a5!n18!c",
    "iban_length": 31,
    "positions": {
      "account_code": [
        9,
        27
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        9
      ]
    }
  },
  "MU": {
    "country": "MU",
    "in_sepa_zone": "false",
    "bban_spec": "4!a2!n2!n12!n3!n3!a",
    "bban_length": 26,
    "iban_spec": "MU2!n4!a2!n2!n12!n3!n3!a",
    "iban_length": 30,
    "positions": {
      "account_code": [
        8,
        26
      ],
      "bank_code": [
        0,
        6
      ],
      "branch_code": [
        6,
        8
      ]
    }
  },
  "NI": {
    "country": "NI",
    "in_sepa_zone": "false",
    "bban_spec": "4!a20!n",
    "bban_length": 24,
    "iban_spec": "NI2!n4!a20!n",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "NL": {
    "country": "NL",
    "in_sepa_zone": "true",
    "bban_spec": "4!a10!n",
    "bban_length": 14,
    "iban_spec": "NL2!n4!a10!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        4,
        14
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "NO": {
    "country": "NO",
    "in_sepa_zone": "true",
    "bban_spec": "4!n6!n1!n",
    "bban_length": 11,
    "iban_spec": "NO2!n4!n6!n1!n",
    "iban_length": 15,
    "positions": {
      "account_code": [
        4,
        11
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "OM": {
    "country": "OM",
    "in_sepa_zone": "false",
    "bban_spec": "3!n16!c",
    "bban_length": 19,
    "iban_spec": "OM2!n3!n16!c",
    "iban_length": 23,
    "positions": {
      "account_code": [
        3,
        19
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "PK": {
    "country": "PK",
    "in_sepa_zone": "false",
    "bban_spec": "4!a16!c",
    "bban_length": 20,
    "iban_spec": "PK2!n4!a16!c",
    "iban_length": 24,
    "positions": {
      "account_code": [
        4,
        20
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "PL": {
    "country": "PL",
    "in_sepa_zone": "true",
    "bban_spec": "8!n16!n",
    "bban_length": 24,
    "iban_spec": "PL2!n8!n16!n",
    "iban_length": 28,
    "positions": {
      "account_code": [
        8,
        24
      ],
      "bank_code": [
        0,
        0
      ],
      "branch_code": [
        0,
        8
      ]
    }
  },
  "PS": {
    "country": "PS",
    "in_sepa_zone": "false",
    "bban_spec": "4!a21!c",
    "bban_length": 25,
    "iban_spec": "PS2!n4!a21!c",
    "iban_length": 29,
    "positions": {
      "account_code": [
        4,
        25
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "PT": {
    "country": "PT",
    "in_sepa_zone": "true",
    "bban_spec": "4!n4!n11!n2!n",
    "bban_length": 21,
    "iban_spec": "PT2!n4!n4!n11!n2!n",
    "iban_length": 25,
    "positions": {
      "account_code": [
        4,
        21
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "QA": {
    "country": "QA",
    "in_sepa_zone": "false",
    "bban_spec": "4!a21!c",
    "bban_length": 25,
    "iban_spec": "QA2!n4!a21!c",
    "iban_length": 29,
    "positions": {
      "account_code": [
        4,
        25
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "RO": {
    "country": "RO",
    "in_sepa_zone": "true",
    "bban_spec": "4!a16!c",
    "bban_length": 20,
    "iban_spec": "RO2!n4!a16!c",
    "iban_length": 24,
    "positions": {
      "account_code": [
        4,
        20
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "RS": {
    "country": "RS",
    "in_sepa_zone": "false",
    "bban_spec": "3!n13!n2!n",
    "bban_length": 18,
    "iban_spec": "RS2!n3!n13!n2!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        3,
        18
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "RU": {
    "country": "RU",
    "in_sepa_zone": "false",
    "bban_spec": "9!n5!n15!c",
    "bban_length": 29,
    "iban_spec": "RU2!n9!n5!n15!c",
    "iban_length": 33,
    "positions": {
      "account_code": [
        14,
        29
      ],
      "bank_code": [
        0,
        9
      ],
      "branch_code": [
        9,
        14
      ]
    }
  },
  "SA": {
    "country": "SA",
    "in_sepa_zone": "false",
    "bban_spec": "2!n18!c",
    "bban_length": 20,
    "iban_spec": "SA2!n2!n18!c",
    "iban_length": 24,
    "positions": {
      "account_code": [
        2,
        20
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "SC": {
    "country": "SC",
    "in_sepa_zone": "false",
    "bban_spec": "4!a2!n2!n16!n3!a",
    "bban_length": 27,
    "iban_spec": "SC2!n4!a2!n2!n16!n3!a",
    "iban_length": 31,
    "positions": {
      "account_code": [
        8,
        27
      ],
      "bank_code": [
        0,
        6
      ],
      "branch_code": [
        6,
        8
      ]
    }
  },
  "SD": {
    "country": "SD",
    "in_sepa_zone": "false",
    "bban_spec": "2!n12!n",
    "bban_length": 14,
    "iban_spec": "SD2!n2!n12!n",
    "iban_length": 18,
    "positions": {
      "account_code": [
        2,
        14
      ],
      "bank_code": [
        0,
        2
      ]
    }
  },
  "SE": {
    "country": "SE",
    "in_sepa_zone": "true",
    "bban_spec": "3!n16!n1!n",
    "bban_length": 20,
    "iban_spec": "SE2!n3!n16!n1!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        3,
        20
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "SI": {
    "country": "SI",
    "in_sepa_zone": "true",
    "bban_spec": "5!n8!n2!n",
    "bban_length": 15,
    "iban_spec": "SI2!n5!n8!n2!n",
    "iban_length": 19,
    "positions": {
      "account_code": [
        5,
        15
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "SK": {
    "country": "SK",
    "in_sepa_zone": "true",
    "bban_spec": "4!n6!n10!n",
    "bban_length": 20,
    "iban_spec": "SK2!n4!n6!n10!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        4,
        20
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "SM": {
    "country": "SM",
    "in_sepa_zone": "true",
    "bban_spec": "1!a5!n5!n12!c",
    "bban_length": 23,
    "iban_spec": "SM2!n1!a5!n5!n12!c",
    "iban_length": 27,
    "positions": {
      "account_code": [
        11,
        23
      ],
      "bank_code": [
        1,
        6
      ],
      "branch_code": [
        6,
        11
      ]
    }
  },
  "SO": {
    "country": "SO",
    "in_sepa_zone": "false",
    "bban_spec": "4!n3!n12!n",
    "bban_length": 19,
    "iban_spec": "SO2!n4!n3!n12!n",
    "iban_length": 23,
    "positions": {
      "account_code": [
        7,
        19
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        7
      ]
    }
  },
  "ST": {
    "country": "ST",
    "in_sepa_zone": "false",
    "bban_spec": "4!n4!n11!n2!n",
    "bban_length": 21,
    "iban_spec": "ST2!n4!n4!n11!n2!n",
    "iban_length": 25,
    "positions": {
      "account_code": [
        8,
        21
      ],
      "bank_code": [
        0,
        4
      ],
      "branch_code": [
        4,
        8
      ]
    }
  },
  "SV": {
    "country": "SV",
    "in_sepa_zone": "false",
    "bban_spec": "4!a20!n",
    "bban_length": 24,
    "iban_spec": "SV2!n4!a20!n",
    "iban_length": 28,
    "positions": {
      "account_code": [
        4,
        24
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "TL": {
    "country": "TL",
    "in_sepa_zone": "false",
    "bban_spec": "3!n14!n2!n",
    "bban_length": 19,
    "iban_spec": "TL2!n3!n14!n2!n",
    "iban_length": 23,
    "positions": {
      "account_code": [
        3,
        19
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "TN": {
    "country": "TN",
    "in_sepa_zone": "false",
    "bban_spec": "2!n3!n13!n2!n",
    "bban_length": 20,
    "iban_spec": "TN2!n2!n3!n13!n2!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        5,
        20
      ],
      "bank_code": [
        0,
        2
      ],
      "branch_code": [
        2,
        5
      ]
    }
  },
  "TR": {
    "country": "TR",
    "in_sepa_zone": "false",
    "bban_spec": "5!n1!n16!c",
    "bban_length": 22,
    "iban_spec": "TR2!n5!n1!n16!c",
    "iban_length": 26,
    "positions": {
      "account_code": [
        5,
        22
      ],
      "bank_code": [
        0,
        5
      ]
    }
  },
  "UA": {
    "country": "UA",
    "in_sepa_zone": "false",
    "bban_spec": "6!n19!c",
    "bban_length": 25,
    "iban_spec": "UA2!n6!n19!c",
    "iban_length": 29,
    "positions": {
      "account_code": [
        6,
        25
      ],
      "bank_code": [
        0,
        6
      ]
    }
  },
  "VA": {
    "country": "VA",
    "in_sepa_zone": "true",
    "bban_spec": "3!n15!n",
    "bban_length": 18,
    "iban_spec": "VA2!n3!n15!n",
    "iban_length": 22,
    "positions": {
      "account_code": [
        3,
        18
      ],
      "bank_code": [
        0,
        3
      ]
    }
  },
  "VG": {
    "country": "VG",
    "in_sepa_zone": "false",
    "bban_spec": "4!a16!n",
    "bban_length": 20,
    "iban_spec": "VG2!n4!a16!n",
    "iban_length": 24,
    "positions": {
      "account_code": [
        4,
        20
      ],
      "bank_code": [
        0,
        4
      ]
    }
  },
  "XK": {
    "country": "XK",
    "in_sepa_zone": "false",
    "bban_spec": "4!n10!n2!n",
    "bban_length": 16,
    "iban_spec": "XK2!n4!n10!n2!n",
    "iban_length": 20,
    "positions": {
      "account_code": [
        4,
        16
      ],
      "bank_code": [
        0,
        2
      ],
      "branch_code": [
        2,
        4
      ]
    }
  }
}


def _convert_bban_spec_to_regex(spec: str) -> str:
    """Converts BBAN spec to regex pattern"""
    spec_to_re = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}
    pattern = re.compile(r"(\d+)(!)?([nace])")
    
    def replacer(match: re.Match) -> str:
        count = int(match.group(1))
        force_exact = match.group(2) == "!"
        char_type = match.group(3)
        
        if force_exact:
            return f"{spec_to_re[char_type]}{{{count}}}"
        return f"{spec_to_re[char_type]}{{1,{count}}}"
    
    return f"^{pattern.sub(replacer, spec)}$"


def _create_iban_specs_table(session: Session):
    """Creates and populates the IBAN_SPECS table with precomputed regex patterns"""
    specs_data = [
        (country, spec["iban_length"], _convert_bban_spec_to_regex(spec["bban_spec"]))
        for country, spec in IBAN_SPECIFICATIONS.items()
    ]

    #TODO utiliser session.create_dataframe
    session.sql("CREATE OR REPLACE TABLE IBAN_SPECS (country_code STRING, iban_length NUMBER, bban_regex STRING)").collect()
    session.sql("DELETE FROM IBAN_SPECS").collect()
    
    for country, length, regex in specs_data:
        session.sql(f"""
            INSERT INTO IBAN_SPECS 
            VALUES ('{country}', {length}, '{regex}')
        """).collect()


def _create_sql_udfs(session: Session):
    """Creates optimized SQL UDFs for IBAN validation"""

    # Validate IBAN Checksum
    session.sql("""
    CREATE OR REPLACE FUNCTION VALIDATE_IBAN_CHECKSUM(IBAN STRING)
    RETURNS BOOLEAN
    AS $$
    SELECT 
        CASE 
            WHEN IBAN IS NULL OR LENGTH(IBAN) < 4 THEN FALSE
            ELSE (
                MOD(
                    TO_NUMBER(
                        REGEXP_REPLACE(
                            UPPER(SUBSTR(IBAN, 5) || SUBSTR(IBAN, 1, 4)),
                            '[A-Z]', 
                            TO_CHAR(ASCII(REGEXP_SUBSTR(UPPER(SUBSTR(IBAN, 5) || SUBSTR(IBAN, 1, 4)), '[A-Z]')) - 55)
                        )
                    ), 
                    97
                ) = 1
            )
        END
    $$
    """).collect()

    # Main IBAN Validation
    session.sql("""
    CREATE OR REPLACE FUNCTION VALIDATE_IBAN(IBAN STRING)
    RETURNS BOOLEAN
    AS $$
    WITH cleaned_iban AS (
        SELECT UPPER(REGEXP_REPLACE(IBAN, '[^A-Z0-9]', '')) AS cleaned
    ),
    country_check AS (
        SELECT 
            cleaned,
            SUBSTR(cleaned, 1, 2) AS country_code,
            LENGTH(cleaned) AS iban_length
        FROM cleaned_iban
    )
    SELECT
        cc.cleaned IS NOT NULL AND
        cc.iban_length = s.iban_length AND
        REGEXP_LIKE(SUBSTR(cc.cleaned, 5), s.bban_regex) AND
        VALIDATE_IBAN_CHECKSUM(cc.cleaned) 
    FROM country_check cc
    LEFT JOIN IBAN_SPECS s 
        ON cc.country_code = s.country_code
    $$
    """).collect()

def ensure_udfs_exist(session: Session):
    """Ensures all required database objects exist"""

    # Check if IBAN_SPECS table exists
    table_exists = session.sql("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = 'IBAN_SPECS'
        ) AS exists
    """).collect()[0]["EXISTS"]
    
    if not table_exists:
        _create_iban_specs_table(session)
        _create_sql_udfs(session)
    else:
        # Check if UDFs exist
        funcs_exist = session.sql("""
            SELECT COUNT(*) = 2 AS all_exist
            FROM information_schema.functions
            WHERE function_name IN ('VALIDATE_IBAN', 'VALIDATE_IBAN_CHECKSUM')
        """).collect()[0]["ALL_EXIST"]
        
        if not funcs_exist:
            _create_sql_udfs(session)

def check_iban(col_name: str) -> Column:
    """
    Public interface for IBAN validation
    Usage: df.withColumn("is_valid_iban", check_iban("iban_column"))
    """
    return call_udf("VALIDATE_IBAN", col(col_name))