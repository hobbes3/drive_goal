#!/usr/bin/env python
# hobbes3

from splunk_rest.splunk_rest import *

def send_and_get_drive_data(arg):
    drive_url = arg["drive_url"]
    drive_type = arg["drive_type"]
    skip = arg["skip"]

    params = {
        "take": DRIVE_TAKE,
        "skip": skip
    }

    r = s.get(drive_url, headers=drive_headers, params=params)

    if r and r.text:
        r_json = r.json()
        rows = r_json.get("data", [])
        log("Current position.", level="debug", extra={"skip": skip, "row_count": len(rows)}, stdout=False)

        data = ""
        for row in rows:
            event = {
                "index": DRIVE_INDEX,
                "sourcetype": "drive_" + drive_type + "_json",
                "source": str(script_file),
                "event": row,
            }

            data += json.dumps(event)

        log("Sending data to Splunk...", stdout=False)
        s.post(HTTP_URL, headers=HTTP_HEADERS, data=data)

@rest_wrapped
def drive_rest_api():
## WORKDAY EMPLOYEES
#    log("Workday: Getting employee list...")
#    workday_auth = HTTPBasicAuth(
#        WORKDAY_USER,
#        WORKDAY_PASS
#    )
#    workday_url = "https://services1.myworkday.com/ccx/service/customreport2/splunk/tetang/SPLK_L_D_Headcount"
#    workday_params = {
#        "format": "json"
#    }
#
#    r = s.get(workday_url, params=workday_params, auth=workday_auth)
#
#    if r:
#        r_json = r.json()
#        employees = r_json["Report_Entry"]
#        employee_count = len(employees)
#
#        log("Found employees.".format(employee_count))
#
#        data = ""
#        for employee in employees:
#            event = {
#                "index": WORKDAY_INDEX,
#                "sourcetype": "workday_employee_json",
#                "source": script_file,
#                "event": employee,
#            }
#
#            data += json.dumps(event)
#
#        logger.info("Workday: Sending data to Splunk...")
#        s.post(HTTP_URL, headers=HTTP_HEADERS, data=data)

# DRIVE GOALS
    arg_list = []

    log("Getting total goals count...")
    drive_url = "https://api.highground.com/1.0/Goals"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)
        log("Found goals.", level="debug", extra={"goal_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "goal",
            "skip": i,
        } for i in range(0, total, DRIVE_TAKE)])

# DRIVE CHECK-INS
    log("Getting total check-ins count...")
    drive_url = "https://api.highground.com/1.0/CheckInSessions"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)

        log("Found check-ins.", level="debug", extra={"checkin_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "checkin",
            "skip": i,
        } for i in range(0, total, DRIVE_TAKE)])

# DRIVE 360 FEEDBACKS
    log("Getting total feedbacks count...")
    drive_url = "https://api.highground.com/1.0/FeedbackSessions"
    drive_params = {
        "take": 1
    }

    r = s.get(drive_url, params=drive_params, headers=drive_headers)

    if r and r.text:
        r_json = r.json()
        total = r_json.get("total", 0)
        log("Found feedbacks.", level="debug", extra={"feedback_total": total})

        arg_list.extend([{
            "drive_url": drive_url,
            "drive_type": "feedback",
            "skip": i,
        } for i in range(0, total, DRIVE_TAKE)])

        if DEBUG:
            arg_list = arg_list[:10]

    log("Getting and sending drive data...")
    for _ in tqdm(pool.imap_unordered(send_and_get_drive_data, arg_list), total=len(arg_list), disable=args.silent):
        pass

if __name__ == "__main__":
    s = retry_session()

    drive_headers = {
        "clientkey": DRIVE_API_KEY,
        "Accept": "application/json",
    }

    drive_rest_api()
