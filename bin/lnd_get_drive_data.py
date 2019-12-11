#!/usr/bin/env python
# hobbes3

import logging
import json
import splunk_rest.splunk_rest as sr
from splunk_rest.splunk_rest import splunk_rest, try_response

def get_drive_data(drive_arg):
    drive_url = drive_arg["drive_url"]
    drive_type = drive_arg["drive_type"]
    skip = drive_arg["skip"]

    drive_params = {
        "take": drive_take,
        "skip": skip
    }

    r = s.get(drive_url, headers=drive_headers, params=drive_params)

    @try_response
    def send_drive_data(r, *args, **kwargs):
        meta = {
            "request_id": r.request_id,
        }

        rows = r.json().get("data", [])
        m = meta.copy()
        m["skip"] = skip
        m["row_count"] = len(rows)
        logger.debug("Current position.", extra=m)

        data = ""
        for row in rows:
            row["splunk_rest"] = {
                "session_id": sr.session_id,
                "request_id": r.request_id,
            }
            event = {
                "index": index,
                "sourcetype": "drive_" + drive_type + "_json",
                "source": __file__,
                "event": row,
            }

            data += json.dumps(event)

        logger.debug("Sending drive data to Splunk...", extra=meta)
        s.post(hec_url, headers=hec_headers, data=data)

    send_drive_data(r)

@splunk_rest
def drive_rest_api():
    @try_response
    def extend_drive_args(r, name, url, *args, **kwargs):
        meta = {
            "request_id": r.request_id,
        }

        total = r.json().get("total", 0)
        m = meta.copy()
        m[name + "_total"] = total
        logger.info("Found {}s.".format(name), extra=m)

        new_args = [{
            "drive_url": url,
            "drive_type": name,
            "skip": i,
        } for i in range(0, total, drive_take)]

        if script_args.sample:
            new_args = new_args[:10]

        drive_args.extend(new_args)

    drive_params = {"take": 1}
    drive_args = []

    logger.info("Getting total goals count...")
    drive_url = "https://api.highground.com/1.0/Goals"
    r = s.get(drive_url, params=drive_params, headers=drive_headers)
    extend_drive_args(r, "goal", drive_url)

    logger.info("Getting total check-ins count...")
    drive_url = "https://api.highground.com/1.0/CheckInSessions"
    r = s.get(drive_url, params=drive_params, headers=drive_headers)
    extend_drive_args(r, "checkin", drive_url)

    logger.info("Getting total feedbacks count...")
    drive_url = "https://api.highground.com/1.0/FeedbackSessions"
    r = s.get(drive_url, params=drive_params, headers=drive_headers)
    extend_drive_args(r, "feedback", drive_url)

    logger.info("Getting drive data...")
    sr.multiprocess(get_drive_data, drive_args)

if __name__ == "__main__":
    script_args = sr.get_script_args()
    logger = logging.getLogger("splunk_rest.splunk_rest")
    s = sr.retry_session()

    drive_headers = sr.config["drive"]["headers"]
    drive_take = sr.config["drive"]["take"]

    hec_url = sr.config["hec"]["url"]
    hec_headers = sr.config["hec"]["headers"]

    index = "main" if script_args.test else sr.config["drive"]["index"]

    drive_rest_api()
