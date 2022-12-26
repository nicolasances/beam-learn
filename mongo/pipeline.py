import apache_beam as beam
import re
import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.mongodbio import ReadFromMongoDB

def main(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument( '--mongouser', default='', help='The mongo user')
    parser.add_argument( '--mongopwd', default='', help='The mongo pwd')

    known_args, pipeline_args = parser.parse_known_args(argv)

    beam_options = PipelineOptions()

    with beam.Pipeline(options = beam_options) as p: 

        p | ReadFromMongoDB(uri = "mongodb+srv://{usr}:{pwd}@pleggit-play-cluster.2xyhv.mongodb.net/admin".format(usr = known_args.mongouser, pwd = known_args.mongopwd), db="profile", coll="profiles")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()