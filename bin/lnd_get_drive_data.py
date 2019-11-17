#!/usr/bin/env python
# hobbes3

import logging
import json
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import rest_wrapped

def send_and_get_drive_data(arg):
    drive_url = arg["drive_url"]
    drive_type = arg["drive_type"]
    skip = arg["skip"]

    params = {
        "take": take,
        "skip": skip
    }

    r = s.get(drive_url, headers=drive_headers, params=params)

    if r and r.text:
        r_json = r.json()
        rows = r_json.get("data", [])
        logger.debug("Current position.", extra={"skip": skip, "row_count": len(rows)})

        data = ""
        for row in rows:
            event = {
                "__session_id": sr.session_id,
                "index": sr.config["drive"]["index"],
                "sourcetype": "drive_" + drive_type + "_json",
                "source": __file__,
                "event": row,
            }

            data += json.dumps(event)

        logger.debug("Sending data to Splunk...")
        s.post(sr.config["hec"]["url"], headers=sr.config["hec"]["headers"], data=data)

@rest_wrapped
def drive_rest_api():
# DRIVE GOALS
    arg_list = []

    logger.info("Getting total goals count...")
    drive_url = "https://api.highground.com/1.0/Goals"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)
        logger.info("Found goals.", extra={"goal_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "goal",
            "skip": i,
        } for i in range(0, total, take)])

# DRIVE CHECK-INS
    logger.info("Getting total check-ins count...")
    drive_url = "https://api.highground.com/1.0/CheckInSessions"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)

        logger.info("Found check-ins.", extra={"checkin_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "checkin",
            "skip": i,
        } for i in range(0, total, take)])

# DRIVE 360 FEEDBACKS
    logger.info("Getting total feedbacks count...")
    drive_url = "https://api.highground.com/1.0/FeedbackSessions"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)
        logger.info("Found feedbacks.", extra={"feedback_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "feedback",
            "skip": i,
        } for i in range(0, total, take)])

        if debug:
            arg_list = arg_list[:10]

    logger.info("Getting and sending drive data...")
    sr.multiprocess(send_and_get_drive_data, arg_list)

if __name__ == "__main__":
    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    debug = sr.config["general"]["debug"]
    drive_headers = sr.config["drive"]["headers"]
    take = sr.config["drive"]["take"]

    drive_rest_api()
