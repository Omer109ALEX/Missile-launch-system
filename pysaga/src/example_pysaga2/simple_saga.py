from typing import Callable, Dict, Optional

from example_pysaga2.actionstep import ActionStep
from example_pysaga2.saga import SagaBuilder


class UploadFileError(Exception):
    pass


class UploadFile(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.file: Optional[str] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__upload_file

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__remove_file_if_exits

    def __upload_file(self, file: str, override: bool = False, **kwargs) -> Dict:
        self.file = file
        print(f'Uploading file override: {override}')
        if 'error' in kwargs:
            raise UploadFileError('This is an error msg!!!')
        return {"uploaded_file": file}

    def __remove_file_if_exits(self, **kwargs) -> bool:
        if self.file:
            print('Removing File')
        return True


class ExecuteFile(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.file: Optional[str] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__run_file

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__stop_file

    def __run_file(self, uploaded_file: str, file_type: str = 'txt', *args, **kwargs) -> Dict:
        self.file = uploaded_file
        print(f'Running file {uploaded_file}.{file_type}')
        return {'running_file': self.file}

    def __stop_file(self, *args, **kwargs) -> bool:
        print(f'file {self.file} stopped')
        return True


if __name__ == '__main__':

    saga = SagaBuilder.create()\
        .action(UploadFile)\
        .action(ExecuteFile)\
        .build()

    result = saga.execute(file='myFile')
    print(result)

    result = saga.execute(file='somefile', error=True)
    print(result)

    def some_action(*args, **kwargs) -> Dict:
        print('Lambda act!')
        return {}

    saga_with_lambda = SagaBuilder.create()\
        .lambda_action(action=some_action,
                       compensation=(lambda *args, **kwargs: True))\
        .build()
    result = saga_with_lambda.execute(file='m43')
    print(result)
