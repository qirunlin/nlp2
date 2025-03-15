import requests
import time
import csv
import re
from datetime import datetime

# ------------------------------
# Configuration
# ------------------------------
# No API key available, so leave as an empty string.
API_KEY = ""
BASE_QUESTIONS_URL = "https://api.stackexchange.com/2.3/questions"
BASE_ANSWERS_URL = "https://api.stackexchange.com/2.3/answers/{}"

# ------------------------------
# Step 1: Retrieve all questions
# ------------------------------
all_questions = []
marker = None   # We'll use this marker (based on creation_date) to retrieve older questions.
batch_num = 1

print("Retrieving questions with tag 'nlp' from Stack Overflow...")

while True:
    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": "nlp",
        "site": "stackoverflow",
        "pagesize": 100,
        "filter": "withbody"
    }
    if API_KEY:
        params["key"] = API_KEY
    if marker is not None:
        # Use marker to fetch only questions older than the current marker (subtract 1 second to avoid duplicates)
        params["max"] = marker - 1

    print(f"\nBatch {batch_num}: Fetching questions with params: {params}")
    response = requests.get(BASE_QUESTIONS_URL, params=params)
    if response.status_code != 200:
        try:
            error_data = response.json()
            if error_data.get("error_name") == "throttle_violation":
                msg = error_data.get("error_message", "")
                match = re.search(r"available in (\d+) seconds", msg)
                if match:
                    wait_time = int(match.group(1))
                    print(f"Throttled: waiting for {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                    continue  # retry the same batch after waiting
                else:
                    print("Throttle violation but could not parse wait time. Waiting 60 seconds.")
                    time.sleep(60)
                    continue
            else:
                print(f"Error: Received status code {response.status_code}")
                print("Response:", response.text)
                break
        except Exception as e:
            print("Error processing error response:", e)
            break

    try:
        data = response.json()
    except Exception as e:
        print("Error parsing JSON:", e)
        break

    items = data.get("items", [])
    print(f"  Retrieved {len(items)} questions in this batch.")
    if not items:
        print("No more questions returned; stopping.")
        break

    all_questions.extend(items)
    
    # Update marker to the smallest (oldest) creation_date in this batch.
    new_marker = min(item["creation_date"] for item in items)
    if marker is not None and new_marker >= marker:
        print("Reached duplicate marker; stopping retrieval.")
        break

    marker = new_marker
    batch_num += 1

    # Respect any backoff instructions
    if "backoff" in data:
        print(f"Backoff instructed: waiting for {data['backoff']} seconds...")
        time.sleep(data["backoff"])
    else:
        time.sleep(0.5)

print(f"\nTotal questions retrieved: {len(all_questions)}")
if not all_questions:
    print("No questions retrieved. Exiting.")
    exit()

# ------------------------------
# Step 2: Retrieve accepted answers
# ------------------------------
# For questions that have an accepted_answer_id, we'll batch-retrieve the answer body.
accepted_ids = {q["question_id"]: q["accepted_answer_id"] for q in all_questions if "accepted_answer_id" in q}
accepted_answer_ids = list(accepted_ids.values())
accepted_answers = {}

print("\nRetrieving accepted answers for questions that have one...")
batch_size = 100  # Up to 100 IDs per API call
for i in range(0, len(accepted_answer_ids), batch_size):
    batch_ids = accepted_answer_ids[i:i+batch_size]
    id_string = ";".join(str(x) for x in batch_ids)
    answers_url = BASE_ANSWERS_URL.format(id_string)
    answer_params = {
        "site": "stackoverflow",
        "pagesize": 100,
        "filter": "withbody"
    }
    if API_KEY:
        answer_params["key"] = API_KEY
    print(f"  Fetching accepted answers for IDs: {id_string}")
    ans_response = requests.get(answers_url, params=answer_params)
    if ans_response.status_code != 200:
        try:
            error_data = ans_response.json()
            if error_data.get("error_name") == "throttle_violation":
                msg = error_data.get("error_message", "")
                match = re.search(r"available in (\d+) seconds", msg)
                if match:
                    wait_time = int(match.group(1))
                    print(f"Throttled (answers): waiting for {wait_time} seconds before retrying batch {id_string}...")
                    time.sleep(wait_time)
                    continue
                else:
                    print("Throttle violation (answers) but could not parse wait time. Waiting 60 seconds.")
                    time.sleep(60)
                    continue
            else:
                print(f"Error: Received status code {ans_response.status_code} for batch {id_string}")
                print("Response:", ans_response.text)
                continue
        except Exception as e:
            print("Error processing error response for answers:", e)
            continue

    try:
        ans_data = ans_response.json()
    except Exception as e:
        print(f"Error parsing JSON for answers batch {id_string}: {e}")
        continue

    for ans in ans_data.get("items", []):
        accepted_answers[ans["answer_id"]] = ans.get("body", "")
    
    if "backoff" in ans_data:
        print(f"Backoff (answers) instructed: waiting for {ans_data['backoff']} seconds...")
        time.sleep(ans_data["backoff"])
    else:
        time.sleep(0.5)

print(f"Finished retrieving accepted answers for {len(accepted_answers)} answers.")

# ------------------------------
# Step 3: Write data to a CSV file
# ------------------------------
# The CSV file will contain:
# - Title
# - Description (question body)
# - Tags (comma-separated)
# - Accepted Answer (if available)
# - is_answered (boolean)
csv_filename = "nlp_questions_with_accepted_answers.csv"
csv_fieldnames = [
    "Title",
    "Description",
    "Tags",
    "Accepted Answer",
    "is_answered"
]

with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_fieldnames)
    writer.writeheader()
    for q in all_questions:
        title = q.get("title", "")
        description = q.get("body", "")
        tags = ", ".join(q.get("tags", []))
        accepted_ans = ""
        if "accepted_answer_id" in q:
            accepted_ans = accepted_answers.get(q["accepted_answer_id"], "")
        is_answered = q.get("is_answered", False)
        writer.writerow({
            "Title": title,
            "Description": description,
            "Tags": tags,
            "Accepted Answer": accepted_ans,
            "is_answered": is_answered
        })

print(f"\nCSV file '{csv_filename}' created with {len(all_questions)} questions.")
