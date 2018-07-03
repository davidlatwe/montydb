
from ..engine.core.field_walker import (
    BaseLogger,
    GetterLogger,
    SetterLogger,
    DropperLogger,
    FieldGetter,
    FieldSetter,
    FieldDropper,
    FieldValues,
    FieldWalker,
)


class MontyWalkerOperationError(Exception):
    pass


class MontyWalker(FieldWalker):

    def __reset(self, deep=False):
        self.value = FieldValues()
        if deep:
            self.log = BaseLogger()

    def get(self):
        try:
            self.log = GetterLogger(self)
        except AttributeError:
            self.__raise_no_go_error()
        FieldGetter().run(self)
        return self

    def set(self, value, by_func=None, pick_with=None):
        # need array_filter_parser
        self.log = SetterLogger(self)
        self.__write_operation_pre_check()
        FieldSetter().run(self, value, by_func, pick_with)
        self.__reset()

    def drop(self, pick_with=None):
        self.log = DropperLogger(self)
        self.__write_operation_pre_check()
        FieldDropper().run(self, pick_with)
        self.__reset()

    def commit(self):
        # down conflict to warning ?
        logger = self.log
        if not isinstance(logger, SetterLogger):
            self.__reset(deep=True)
            self.__raise_no_transaction_error()
        result = []
        for transaction in logger.transaction_queue:
            result.append(transaction())
        self.__reset(deep=True)  # set back to getterlogger?
        return any(result)

    def __write_operation_pre_check(self):
        if self.log.field_levels is None:
            self.__reset(deep=True)
            self.__raise_no_go_error()

    def __raise_no_go_error(self):
        raise MontyWalkerOperationError(
            "Field path not given, run `go` first.")

    def __raise_no_transaction_error(self):
        raise MontyWalkerOperationError(
            "No transaction, run `set` or `drop` first.")
