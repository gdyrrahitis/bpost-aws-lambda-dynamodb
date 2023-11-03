import boto3
import os
import csv

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ.get("AWS_MOVIES_TABLE_NAME"))
s3 = boto3.client("s3")

def lambda_handler(event, context):
    print(f"Event {event}")
    print(f"Context {context}")
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            movies = read_csv_body(response)
            write_to_dynamodb(movies)
        except Exception as e:
            print(e)
            print("Error getting object {} from bucket {}".format(key, bucket))
            raise e

def write_to_dynamodb(movies):
    print("Writing to dynamodb")
    with table.batch_writer() as batch:
        for movie in movies:
            batch.put_item(Item=movie)
        print(f"Finished writing to {table.table_name}")

def read_csv_body(response):
    print("start reading csv file")
    movies = []
    body = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.DictReader(body)
    for row in reader:
        data = {}
        data["Year"] = int(row["Year"])
        data["Title"] = row["Title"]
        data["Meta"] = {
            "Length": int(row["Length"] or 0),
            "Subject": row["Subject"] or None,
            "Actor": row["Actor"] or None,
            "Actress": row["Actress"] or None,
            "Director": row["Director"] or None,
            "Popularity": row["Popularity"] or None,
            "Awards": row["Awards"] or None,
            "Image": row["Image"] or None
        }
        movies.append(data)
    print(f"Finished adding movies, added {len(movies)} movies.")
    return movies
