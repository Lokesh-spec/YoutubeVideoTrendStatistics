import re
import csv
import json
import datetime
import apache_beam as beam


# Custom Transform for Splitting and Parsing CSV Line
class SplitAndParseVideoCSVLine(beam.DoFn):
    
    def __init__(self):
        self.video_id = set()
    
    def process(self, element):
        try:
            csv_reader = csv.reader([element], delimiter=',', quotechar='"')
            for row in csv_reader:
                if 'video_id' in row:
                    continue
                # Ensure the row has the expected number of fields
                if len(row) < 16:
                    print(f"Skipping malformed row: {row}")
                    continue
                
                if row[0] not in self.video_id:
                    self.video_id.add(row[0])
                else:
                    continue      
                # Handle trending_date
                try:
                    if not row[1].startswith("20"):
                        trending_date_parts = row[1].split(".")
                        trending_date = "-".join(
                            ["20" + trending_date_parts[0], trending_date_parts[2], trending_date_parts[1]]
                        )
                        trending_date = datetime.datetime.strptime(trending_date, "%Y-%m-%d")
                    else:
                        trending_date_parts = row[1].split(".")
                        trending_date = "-".join(
                            [trending_date_parts[0], trending_date_parts[2], trending_date_parts[1]]
                        )
                        trending_date = datetime.datetime.strptime(trending_date, "%Y-%m-%d")
                except Exception as e:
                    print(f"Error parsing trending_date for row: {row} - {e}")
                    continue
                
                # Handle publish_time
                try:
                    publish_time = datetime.datetime.strptime(row[5], "%Y-%m-%dT%H:%M:%S.%fZ")
                except Exception as e:
                    print(f"Error parsing publish_time for row: {row} - {e}")
                    continue
                
                # Convert boolean fields
                indices = [12, 13, 14]
                for idx in indices:
                    row[idx] = row[idx] == "True"
                
                # Yield parsed data as a dictionary
                yield {
                    'video_id': row[0],
                    'trending_date': trending_date,
                    'title': row[2],
                    'channel_title': row[3],
                    'category_id': row[4],
                    'publish_time': publish_time,
                    'tags': row[6].replace('"', ""),
                    'views': int(row[7]) if row[7].isdigit() else 0,
                    'likes': int(row[8]) if row[8].isdigit() else 0,
                    'dislikes': int(row[9]) if row[9].isdigit() else 0,
                    'comment_count': int(row[10]) if row[10].isdigit() else 0,
                    'thumbnail_link': row[11],
                    'comments_disabled': row[12],
                    'ratings_disabled': row[13],
                    'video_error_or_removed': row[14],
                    'description': row[15]
                }
        except Exception as e:
            print(f"Error processing line: {element} - {e}")

# Custom Transform for Parsing Category CSV Line
class SplitAndParseCategoryCSVLine(beam.DoFn):
    def process(self, element):
        try:
            csv_reader = csv.reader([element], delimiter=',', quotechar='"')
            for row in csv_reader:
                # Ensure there are enough columns before processing
                if len(row) < 6:
                    print(f"Skipping malformed category row: {row}")
                    continue
                yield (row[2].strip(), row[4].strip()) 
        except Exception as e:
            print(f"Error processing category line: {element} - {e}")
            return []

class FormatBigQueryFormat(beam.DoFn):
    def process(self, element):
        yield {
            'video_id': element[0],
            'trending_date': element[1],
            'title': element[2],
            'category_id': element[3],
            'publish_time': element[4],
            'views': element[5],
            'likes': element[6],
            'dislikes': element[7],
            'comment_count': element[8],
            'thumbnail_link': element[9],
            'comments_disabled': element[10],
            'tags': element[11],
            'ratings_disabled': element[12],
            'video_error_or_removed': element[13],
            'description': element[14],
            'category_name': element[15],
            'channel_title' : element[16]
        }

def map_video_with_category(video, categories):
    category_name = categories.get(video["category_id"], "Unknown")
    video["category_name"] = category_name
    return video

