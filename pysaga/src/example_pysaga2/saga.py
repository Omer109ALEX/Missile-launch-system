from typing import List, Callable, Type, Optional, Dict

from example_pysaga2.actionstep import ActionStep, LambdaActionStep, ActionError


class SagaCompensationError(Exception):
    """
    SagaCompensationError in the Exception type raised when an error occurred in one of the
    Saga's compensations. Can be used for a retry mechanism and logging compensations errors.
    """
    def __init__(self, compensation_exception: List[Exception],
                 *args, **kwargs):
        """
        :param compensation_exception: The exceptions raised in one or more compensations.
        :param args: Additional args.
        :param kwargs: Additional kwargs.
        """
        super().__init__(args, kwargs)
        self.compensation_exceptions = compensation_exception


# TODO: refactor the SagaResult - add more options and write it better.
class SagaResult:
    """
    Used for returning the result (success/failure) of the Saga execution.
    In case of successes additional arguments can be added using result_args.
    In case of failure data will be stored in saga_error, the status of the compensations (success/failure)
    will be stored in compensation_success and in case of error in saga_compensation_error.
    """
    def __init__(self):
        self.success: Optional[bool] = True
        self.compensations_success: bool = True
        self.result_args: Dict = {}
        self.action_error: Optional[ActionError] = None
        self.saga_compensation_error: Optional[SagaCompensationError] = None

    def __str__(self):
        msg = f"Saga Success Result: {self.success}"
        msg = f"{msg}\nSaga Exception Message: {str(self.action_error)}" \
            if not self.success  else msg
        msg = f"{msg}\nAdditional Compensations Errors : {str(self.saga_compensation_error)}" \
            if not self.compensations_success else msg
        return msg


class Saga:
    """
    A series of ActionSteps that can be executed.
    """

    def __init__(self, action_steps: List[ActionStep]):
        self.action_steps = action_steps

    def execute(self, **first_action_args) -> SagaResult:
        """
        Execute the actions of the Saga.
        Each action will get the arguments from the result arguments of the last step (arguments chain)
        which will override the Action arguments.
        If one of the ActionSteps fails in execution, the Saga will revert changes by executing compensations in
        reverse order.
        :param first_action_args: arguments which can be passed to first action in the chain.
        :return: SagaResult that includes success status and additional info.
        """
        saga_action_args = first_action_args
        actions_done: List[ActionStep] = []
        saga_result = SagaResult()

        for action in self.action_steps:
            # Action is added to actions_done before execution to run compensation on failed action too.
            actions_done.append(action)
            try:
                saga_action_args = action.act(**saga_action_args) or {}
            except ActionError as e:
                saga_result.success = False
                saga_result.action_error = e
                return self.__run_compensation(actions_done, saga_result)

            saga_result.result_args.update(saga_action_args)

        saga_result.success = True
        return saga_result

    @staticmethod
    def __run_compensation(action_steps: List[ActionStep],
                           saga_result: SagaResult) -> SagaResult:
        """
        Execute the compensations of the ActionSteps.
        :param action_steps: List of ActionSteps that need to reverted.
        :param saga_result: Already created SagaResult that includes additional info on the Saga failure.
        :return: Updated SagaResult with compensation information.
        """
        saga_compensation_error: Optional[SagaCompensationError] = None
        for action in action_steps:
            try:
                action.compensate()
            except Exception as e:
                # TODO: you can add a retry mechanism on a failed compensation here.
                if saga_compensation_error:
                    saga_compensation_error.compensation_exceptions.append(e)
                else:
                    saga_compensation_error = SagaCompensationError(compensation_exception=[e])

        if saga_compensation_error:
            saga_result.compensations_success = False
            saga_result.saga_compensation_error = saga_compensation_error
        return saga_result


class SagaBuilder:
    """
    SagaBuilder is a class used for creating a Saga with multiple ActionSteps.
    The SagaBuilder is following the Builder design pattern.
    example of usage:

        saga = SagaBuilder.create() \
            .action(SomeActionStep1) \
            .action(SomeActionStep2, my_arg=val) \
            .lambda_action(callable_action, callable_compensation, arg=val2) \
            .build()

        saga.execute()
    """

    def __init__(self, action_steps: List[ActionStep] = None, **default_args):
        self.__action_steps: List[ActionStep] = action_steps or []
        self.__default_args: Dict = default_args

    @staticmethod
    def create(action_steps: List[ActionStep] = None, **default_args):
        """
        First initializing of the SagaBuilder, used for method chaining.
        :param action_steps: SagaBuilder can be initialized with a list of ActionSteps.
        :param default_args: Default args that will be passed to all of the actions in the Saga.
        :return: A SagaBuilder instance.
        """
        return SagaBuilder(action_steps=action_steps, **default_args)

    def lambda_action(self, action: Callable, compensation: Callable, **action_kwargs) -> "SagaBuilder":
        """
        Add a lambda action to the Saga built.
        :param action: The method of the action itself.
        :param compensation: The method that reverses the effects of the action after an exception is raised.
        :param action_kwargs: Additional args passed to the action and saved in as part of the ActionStep metadata.
        The action_kwargs will override the existing default_kwargs saved in builder.
        :return: SagaBuilder for method chaining.
        """
        args = {**self.__default_args, **action_kwargs}
        action = LambdaActionStep(action=action,
                                  compensation=compensation,
                                  **args)
        self.__action_steps.append(action)
        return self

    # TODO: action state is saved between executions, this should be avoided.
    def action(self, action_type= Type[ActionStep],
               **action_kwargs) -> "SagaBuilder":
        """
        Add an action to the Saga built.
        :param action_type: The ActionStep class that will be added to the saga.
        :param action_kwargs: Additional args passed to the action and saved in as part of the ActionStep metadata.
        The action_kwargs will override the existing default_kwargs saved in builder.
        :return: SagaBuilder for method chaining.
        """
        args = {**self.__default_args, **action_kwargs}
        action = action_type(**args)
        self.__action_steps.append(action)
        return self

    def build(self) -> Saga:
        """
        Finish the build process by creating the Saga from thr ActionSteps collected.
        :return: A Saga instance with all the ActionSteps added to the SagaBuilder.
        """
        return Saga(self.__action_steps)
