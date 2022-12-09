import time
import asyncio
from typing import Callable, Dict, List, Optional, Union
from fastapi import FastAPI, Header
from pydantic import BaseModel
from example_pysaga2 import saga, actionstep, simple_saga
from example_pysaga2.actionstep import ActionStep
from example_pysaga2.saga import SagaBuilder


app = FastAPI()
# missiles_db example = missiles types : number of missiles from this type
missiles_db = {"dr3": 3, "patriot": 2}
# users_db example = user_id : List[missile_type_with_permission]
users_db = {123: ["dr3", "patriot"], 12: ["patriot"], 1: []}
# target_db example = target_name : target_coordinate
target_db = {"nasrala": "N32404 E35105", "omer": None}

class Item(BaseModel):
    target: str
    missile_type: str
    pause: int


@app.post("/items/")
async def create_item(item: Item):
    return item


@app.get("/missile/{missile_type}")
async def missile_registry(missile_type: str):
    if not missiles_db.keys().__contains__(missile_type):
        return False
    if missiles_db[missile_type] > 0:
        msg = f"Missile {missile_type} is registered"
        print(msg)
        return msg
    else:
        return False  # no missiles from this type


@app.post("/missile/{missile_type}/fire/{coordinate}")
def missile_launch(missile_type: str, coordinate: str):
    if not missiles_db.keys().__contains__(missile_type):
        return False
    if missiles_db[missile_type] > 0:
        missiles_db[missile_type] -= 1

        if not coordinate == "":
            msg = f"Missile {missile_type} lunched to the coordinate: {coordinate}"
            print(msg)
            return msg

    return False


@app.get("/user/{id}/missile/{missile}")
def get_user_permission_by_missile_type(id: int, missile: str):
    if not users_db.keys().__contains__(id):
        return False
    if users_db[id].__contains__(missile):
        msg = f"User id {id} have permission to missile {missile}"
        print(msg)
        return msg
    return False


@app.get("/location/{target_name}")
def get_coordinate_of_location(target_name: str):
    if not target_db.keys().__contains__(target_name):
        return False
    else:
        msg = f"Target {target_name} location is in coordinate: {target_db[target_name]}"
        print(msg)
        return target_db[target_name]



@app.get("/startSaga/{user_id}/{target_name}/{missile_type}/{pause}")
async def start_saga(user_id: int, target_name: str, missile_type: str, pause: int):

    saga = SagaBuilder.create() \
        .action(RegisterMissile) \
        .action(TargetLocation) \
        .action(PermissionCheck) \
        .action(MissileLaunch) \
        .build()

    print("Starting the Saga -------------------------------------------")
    result = saga.execute(user_id=user_id,
                          target_name=target_name,
                          missile_type=missile_type,
                          pause=pause)
    print(result)
    print("Finish the Saga ---------------------------------------------")
    return result


class MissileError(Exception):
    pass


class TargetError(Exception):
    pass


class PermissionError(Exception):
    pass


class LaunchError(Exception):
    pass


class RegisterMissile(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.missile_type: Optional[str] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__register_missile

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__rollback_register_missile

    def __register_missile(self, user_id: int, target_name: str,
                           missile_type: str, pause: int,
                           *args, **kwargs) -> Dict:

        if missile_registry(missile_type):
            return {"user_id": user_id, "target_name": target_name,
                    "missile_type": missile_type, "pause": pause}

        print(f'Failed to register missile {missile_type}')
        raise MissileError(f'Missile {self.missile_type} doesnt exists in storage')

    def __rollback_register_missile(self, **kwargs) -> bool:
        print('Rollback from missile registered')
        return True


class TargetLocation(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.target_name: Optional[str] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__get_target_location

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__target_location_rollback

    def __get_target_location(self, user_id: int, target_name: str,
                           missile_type: str, pause: int,
                           *args, **kwargs) -> Dict:

        target_coordinate = get_coordinate_of_location(target_name)
        if target_coordinate:
            return {"user_id": user_id, "target_name": target_name,
                    "missile_type": missile_type, "pause": pause,
                    "target_coordinate": target_coordinate}

        print(f'Failed to find target {target_name} location')
        raise TargetError(f'Target {self.target_name} doesnt found')

    def __target_location_rollback(self, *args, **kwargs) -> bool:
        print('Rollback from find target location')
        return True


class PermissionCheck(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.user_id: Optional[int] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__check_permission

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__permission_rollback

    def __check_permission(self, user_id: int, target_name: str,
                           missile_type: str, pause: int, target_coordinate: str,
                           *args, **kwargs) -> Dict:

        if get_user_permission_by_missile_type(user_id, missile_type):
            return {"user_id": user_id, "target_name": target_name,
                    "missile_type": missile_type, "pause": pause,
                    "target_coordinate": target_coordinate}

        print(f'Failed with {user_id} permission to missile {missile_type}')
        raise PermissionError(f'UserId: {self.user_id} , doesnt have permission to {self.missile_type}')

    def __permission_rollback(self, *args, **kwargs) -> bool:
        print("Rollback from permission confirmed")
        return True


class MissileLaunch(ActionStep):
    def __init__(self, **action_step_kwargs: Dict):
        super().__init__(**action_step_kwargs)
        self.missile_type: Optional[str] = None
        self.target_coordinate: Optional[str] = None

    @property
    def _action(self) -> Callable[..., Dict[any, any]]:
        return self.__missile_launch

    @property
    def _compensation(self) -> Callable[..., bool]:
        return self.__stop_launch

    def __missile_launch(self, user_id: int, target_name: str,
                           missile_type: str, pause: int, target_coordinate: str,
                           *args, **kwargs) -> Dict:
        self.missile_type = missile_type
        self.target_coordinate = target_coordinate

        time.sleep(pause)  # wait X sec, X is from request

        if missile_launch(missile_type, target_coordinate):
            return {"user_id": user_id, "target_name": target_name,
                    "missile_type": missile_type, "pause": pause,
                    "target_coordinate": target_coordinate}

        print(f'Failed to launch {missile_type} to the coordinate {target_coordinate}')
        raise LaunchError(f'Can not launch the missile {missile_type} to {target_coordinate}')

    def __stop_launch(self, *args, **kwargs) -> bool:
        print("Rollback after launch")
        return True

