import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from transforms.custom_transform import SplitAndParseVideoCSVLine, SplitAndParseCategoryCSVLine, FormatBigQueryFormat, map_video_with_category

def run_pipeline(config_data, beam_args):
    
    # Retrieve paths from the config
    video_csv_path = config_data.get('inputs', {}).get('video_csv_path')
    
    category_csv_path = config_data.get('inputs', {}).get('category_csv_path') 
    
    
    output_file_path = config_data.get('output', {}).get('path')
    
    
    # Validate file paths
    # if not video_csv_path or not os.path.exists(video_csv_path):
    #     raise ValueError(f"CSV path for video data is missing or invalid: {video_csv_path}")
    
    # if not category_csv_path or not os.path.exists(category_csv_path):
    #     raise ValueError(f"CSV path for category data is missing or invalid: {category_csv_path}")
    
    
    with beam.Pipeline(options=PipelineOptions(beam_args)) as p:
        
        
        # Reading and parsing video data
        video_csv_input = (
            p
            | "Read From Video CSV" >> beam.io.ReadFromText(video_csv_path)
            | "Split and Parse Video CSV Line" >> beam.ParDo(SplitAndParseVideoCSVLine())
            # | beam.Map(print)
        )
        
        # Reading and parsing category data
        category_csv_input = (
            p
            | "Read From Category CSV" >> beam.io.ReadFromText(category_csv_path)
            | "Parse Category CSV" >> beam.ParDo(SplitAndParseCategoryCSVLine())
        )
        
        

        categories_side_input  = beam.pvalue.AsDict(category_csv_input)
        
        enriched_videos = (
            video_csv_input
            | "Map Video with Category" >> beam.Map(map_video_with_category, categories=categories_side_input)
        )

        fact_videos_biquery = (
            enriched_videos 
            | 'Select Specific Data for Fact' >> beam.Map(lambda element: (element['video_id'], element['trending_date'], element['title'], element['category_id'], element['publish_time'], element['views'], element['likes'], element['dislikes'], element['comment_count'], element['thumbnail_link'], element['comments_disabled'],  element['tags'], element['ratings_disabled'], element['video_error_or_removed'], element['description'], element['category_name'], element['channel_title']))
            | 'Format to bigquery' >>  beam.ParDo(FormatBigQueryFormat())
            | "Write Enriched Data" >> beam.io.WriteToText(output_file_path)
        )
        
