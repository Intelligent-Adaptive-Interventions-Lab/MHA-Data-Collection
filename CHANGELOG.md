# CHANGELOG.md
## Template - DD/MM/YYYY [Name]
- [Added]
- [Deleted]
- [Testing]
- [Edited]

## 25/09/2021 [Jiakai]
- [Added] Modification in `DataSummarizer/MturkDataSummarizer.py`:
            Due to there is a difference in assignment batch size and update batch size,
            Summarizer will drop rows with NAs in batch_group_updated, which means that
            it will compute the summary with observarions in the update batch.

## 21/09/2021 [Koby]
- [Added] Concrete implementation of `DataSummarizer/MturkDataSummarizer.py`
- [Added] Define contracts in `DataSummarizer/DataSummarizer.py`

## 20/09/2021 [Koby]
- [Added] Concrete implementation of `DataPipeine/mturk_data_pipeline.py`
- [Added] utils functions in `DataPipeline/utils.py`

## 19/09/2021 [Koby]
- [Added] Concrete Implementation of `DataPipeline/mha_data_pipeline.py`

## 18/09/2021 [Koby]
- [Added] Concrete Implementation of `DataPipelines/mha_dialog_pipeline.py`
- [Added] Define new exceptions of `Exceptions/DataPipelineExceptions.py`

## 13/09/2021 [Koby]
- [Added] Concrete implementation of `DataDownloader/moocletdatadownloader.py`
- [Added] `__init__.py` in all the folders
- [Added] `.gitignore` to prevent git updates on `APIConfigs/secure.py`
- [Added] Define New exceptions in `Exceptions/DataDownloaderException.py`
- [Added] Concrete implementation of `DataDownloader/mhadatadownloader.py`

## 11/09/2021 [Koby]
- [Added] API configurations for `APIConfigs/mha_controller.yaml` and `APIConfigs/mooclet.yaml`
- [Added] Abstract methods for `DataDownloader/datadownloader.py`
