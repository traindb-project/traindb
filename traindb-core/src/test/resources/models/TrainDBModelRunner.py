"""
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import importlib
import json
import os

class TrainDBModelRunner():

  def _load_module(self, modeltype_class, modeltype_path):
    spec = importlib.util.spec_from_file_location(modeltype_class, modeltype_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return mod

  def train_model(self, modeltype_class, modeltype_path, real_data, table_metadata, model_path, args=[], kwargs={}):
    mod = self._load_module(modeltype_class, modeltype_path)
    model = getattr(mod, modeltype_class)(*args, **table_metadata['options'])
    model.train(real_data, table_metadata)
    model.save(model_path)

    train_info = {}
    train_info['base_table_rows'] = len(real_data.index)
    train_info['trained_rows'] = len(real_data.index)
    return json.dumps(train_info)

  def generate_synopsis(self, modeltype_class, modeltype_path, model_path, row_count):
    mod = self._load_module(modeltype_class, modeltype_path)
    model = getattr(mod, modeltype_class)()
    model.load(model_path)
    syn_data = model.synopsis(row_count)
    return syn_data


import argparse
import pandas as pd
import json
import sys

root_parser = argparse.ArgumentParser(description='TrainDB Model Runner')
subparsers = root_parser.add_subparsers(dest='cmd')
parser_train = subparsers.add_parser('train', help='train model command')
parser_train.add_argument('modeltype_class', type=str, help='(str) modeltype class name')
parser_train.add_argument('modeltype_uri', type=str, help='(str) path for local model, or uri for remote model')
parser_train.add_argument('data_file', type=str, help='(str) path to .csv data file')
parser_train.add_argument('metadata_file', type=str, help='(str) path to .json table metadata file')
parser_train.add_argument('model_path', type=str, help='(str) path to model')

parser_synopsis = subparsers.add_parser('synopsis', help='generate synopsis command')
parser_synopsis.add_argument('modeltype_class', type=str, help='(str) modeltype class name')
parser_synopsis.add_argument('modeltype_uri', type=str, help='(str) path for local model, or uri for remote model')
parser_synopsis.add_argument('model_path', type=str, help='(str) path to model')
parser_synopsis.add_argument('row_count', type=int, help='(int) the number of rows to generate')
parser_synopsis.add_argument('output_file', type=str, help='(str) path to save generated synopsis file')

args = root_parser.parse_args()
runner = TrainDBModelRunner()
if args.cmd == 'train':
  data_file = pd.read_csv(args.data_file)
  with open(args.metadata_file) as metadata_file:
    table_metadata = json.load(metadata_file)
  json_train_info = runner.train_model(args.modeltype_class, args.modeltype_uri, data_file, table_metadata, args.model_path)
  with open(os.path.join(args.model_path, 'train_info.json'), 'w') as f:
    f.write(json_train_info)
  sys.exit(0)
elif args.cmd == 'synopsis':
  syn_data = runner.generate_synopsis(args.modeltype_class, args.modeltype_uri, args.model_path, args.row_count)
  syn_data.to_csv(args.output_file, index=False)
  sys.exit(0)
else:
  root_parser.print_help()
