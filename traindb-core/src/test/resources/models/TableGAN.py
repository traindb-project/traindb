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
from sdgym.synthesizers import TableGAN as SDGymTableGAN
from sdgym.errors import UnsupportedDataset
from TrainDBBaseModel import TrainDBModel, TrainDBSynopsisModel
import pandas as pd

import torch

LOGGER = logging.getLogger(__name__)

class TableGAN(TrainDBSynopsisModel, SDGymTableGAN):

    def __init__(self,
                 random_dim=100,
                 num_channels=64,
                 l2scale=1e-5,
                 batch_size=500,
                 epochs=300):
 
        self.ht = rdt.HyperTransformer(default_data_type_transformers={
            'categorical': 'LabelEncodingTransformer',
        })
        self.columns = []

        super().__init__(random_dim, num_channels, l2scale, batch_size, epochs)

    def train(self, real_data, table_metadata):
        columns, categoricals = self.get_columns(real_data, table_metadata)
        real_data = real_data[columns]
        self.columns = columns

        self.ht.fit(real_data.iloc[:, categoricals])
        model_data = self.ht.transform(real_data)

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
        }, output_path + '/model.pth')

    def load(self, input_path):
        saved_model = torch.load(input_path + '/model.pth')
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
        sampled_data = pd.DataFrame(sampled_data, columns=self.columns)

        synthetic_data = self.ht.reverse_transform(sampled_data)
        return synthetic_data
