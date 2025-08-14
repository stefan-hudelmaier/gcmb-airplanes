* This project shall connect to a TCP socket with SBS1 data and extract airplane positions.
* The file sbs1.py contains support for parsing SBS1 data
* The file legacy-main.py contains the code of a different project that does exactly the same thing: Read data
  from a socket via SBS1. It can be used as a reference, but the logic should be re-implemented in the file main.py
  using the instructions and rules outlined.
* The MQTT messages must be published with the retain flag set to true, but with limited retention time of 10 minutes.
