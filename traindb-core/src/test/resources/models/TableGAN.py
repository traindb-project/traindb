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

import logging
import rdt
from rdt.transformers import *
from tablegan.tablegan import TableGAN as SDGymTableGAN
from tablegan.errors import UnsupportedDataset
import os
import pandas as pd
import torch
from TrainDBBaseModel import TrainDBModel, TrainDBSynopsisModel

LOGGER = logging.getLogger(__name__)

class TableGAN(TrainDBSynopsisModel, SDGymTableGAN):

    def __init__(self,
                 random_dim=100,
                 num_channels=64,
                 l2scale=1e-5,
                 batch_size=500,
                 epochs=300):
 
        self.ht = rdt.HyperTransformer()
        self.columns = []

        super().__init__(random_dim, num_channels, l2scale, batch_size, epochs)

    def _get_hyper_transformer_config(self, real_data, table_metadata):
        sdtypes = {}
        transformers = {}

        fields_meta = table_metadata['fields']

        for column in real_data.columns:
            field_meta = fields_meta[column]
            field_type = field_meta['type']
            if field_type == 'id':
                continue

            if field_type == 'categorical':
                sdtypes[column] = 'categorical'
                transformers[column] = LabelEncoder()
            elif field_type == 'numerical':
                sdtypes[column] = 'numerical'
                transformers[column] = FloatFormatter()
            elif field_type == 'datetime':
                sdtypes[column] = 'datetime'
                transformers[column] = UnixTimestampEncoder()
            elif field_type == 'boolean':
                sdtypes[column] = 'boolean'
                transformers[column] = BinaryEncoder()

        return {'sdtypes': sdtypes, 'transformers': transformers}

    def train(self, real_data, table_metadata):
        columns, categoricals = self.get_columns(real_data, table_metadata)
        real_data = real_data[columns]
        self.columns = columns

        ht_config = self._get_hyper_transformer_config(real_data, table_metadata)
        self.ht.set_config(ht_config)
        model_data = self.ht.fit_transform(real_data)

        supported = set(model_data.select_dtypes(('number', 'bool')).columns)
        unsupported = set(model_data.columns) - supported
        if unsupported:
            unsupported_dtypes = model_data[unsupported].dtypes.unique().tolist()
            raise UnsupportedDataset(f'Unsupported dtypes {unsupported_dtypes}')

        nulls = model_data.isnull().any()
        if nulls.any():
            unsupported_columns = nulls[nulls].index.tolist()
            raise UnsupportedDataset(f'Null values found in columns {unsupported_columns}')

        LOGGER.info("Training %s", self.__class__.__name__)
        self.fit(model_data.to_numpy(), categoricals, ())

    def save(self, output_path):
        torch.save({
            'ht': self.ht,
            'transformer': self.transformer,
            'generator': self.generator,
            'columns': self.columns
        }, os.path.join(output_path, 'model.pth'))

    def load(self, input_path):
        saved_model = torch.load(os.path.join(input_path, 'model.pth'))
        self.ht = saved_model['ht']
        self.transformer = saved_model['transformer']
        self.generator = saved_model['generator']
        self.columns = saved_model['columns']

    def list_hyperparameters():
        hparams = []
        hparams.append(TrainDBModel.createHyperparameter('random_dim', 'int', '100', 'the size of the random sample passed to the generator'))
        hparams.append(TrainDBModel.createHyperparameter('num_channels', 'int', '64', 'the number of channels'))
        hparams.append(TrainDBModel.createHyperparameter('l2scale', 'float', '1e-5', 'regularization term'))
        hparams.append(TrainDBModel.createHyperparameter('batch_size', 'int', '500', 'the number of samples to process in each step'))
        hparams.append(TrainDBModel.createHyperparameter('epochs', 'int', '300', 'the number of training epochs'))
        return hparams

    def synopsis(self, row_count):
        LOGGER.info("Synopsis Generating %s", self.__class__.__name__)
        sampled_data = self.sample(row_count)
        # hack: concat '.value' suffix to avoid the bug in RDT 1.2.1
        ht_columns = ['{}{}'.format(col, '.value') for col in self.columns]
        sampled_data = pd.DataFrame(sampled_data, columns=ht_columns)

        synthetic_data = self.ht.reverse_transform(sampled_data)
        return synthetic_data
