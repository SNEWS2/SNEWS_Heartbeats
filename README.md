# SNEWS_Heartbeats
Detector Heartbeats Handler.

This module serves as a backend to tracka and handle the detector heartbeats.

after installation <br>
`user$:pip install ./`

run CLI to start the heartbeat server <br>

`snews_hb run-hearbeat` <br>

You can specify the logs folder via `-o` flag, and what to store via `-s` you have three options
`['csv','json','both']` default is both. 

For the moment, it stores the beats in both json and csv file **for every day separately** (continuosly appending during the same day).

It also saves and keeps appending to a complete csv file for now. 

Example table (manually submitted example heartbeat a couple of time).

 |    |             Received Times | Detector   | Stamped Times       | Latency                | Time After Last |                                                                                             
 |---------------------------:|:---------------------------|:-----------|:--------------------|:----------------|:------------------|
 |  0 | 2022-04-22 11:22:48.547809 | TEST       | 2022-04-22 11:22:44 | 0 days 00:00:04.547809 | 0 days 00:00:00        |
 |  1 | 2022-04-22 11:22:58.589410 | TEST       | 2022-04-22 11:22:54 | 0 days 00:00:04.589410 | 0 days 00:00:10.041601 |
 |  2 | 2022-04-22 11:23:03.616164 | TEST       | 2022-04-22 11:22:59 | 0 days 00:00:04.616164 | 0 days 00:00:05.026754 |

