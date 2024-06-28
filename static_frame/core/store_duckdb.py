import typing_extensions as tp

from static_frame.core.generic_aliases import TFrameAny
from static_frame.core.store import Store

if tp.TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


# '''
# WITH
# t1 AS (SELECT ROW_NUMBER() OVER() AS rownum, * FROM a1),
# t2 AS (SELECT ROW_NUMBER() OVER() AS rownum, * FROM a2),
# t3 AS (SELECT ROW_NUMBER() OVER() AS rownum, * FROM a3)
# SELECT t1.column0 AS int_column, t2.column0 AS bool_column, t3.column0 AS str_column
# FROM t1
# JOIN t2 ON t1.rownum = t2.rownum
# JOIN t3 ON t1.rownum = t3.rownum
# '''



class StoreDuckDB(Store):

    @classmethod
    def _frame_to_connection(cls,
            *,
            frame: TFrameAny,
            label: str, # can be None
            connection: 'DuckDBPyConnection',
            include_index: bool,
            include_columns: bool,
            ) -> 'DuckDBPyConnection':
        '''
        Args:
            label: string to be used as the table name.
        '''
        field_names, _ = cls.get_field_names_and_dtypes(
                frame=frame,
                include_index=include_index,
                include_index_name=True,
                include_columns=include_columns,
                include_columns_name=False,
                force_brackets=False
                )

        label_arrays = zip(field_names,
                cls.get_column_iterator(frame, include_index=include_index)
                )

        query = [f'CREATE TABLE {label} AS WITH']
        w = []
        s = ['SELECT']

        for i, (label, array) in enumerate(label_arrays):
            exec(f'a{i} = array')
            w.append(f't{i} AS (SELECT ROW_NUMBER() OVER() AS rownum, * FROM a{i})')
            s.append(f't{i}.column0 AS {label},')

        query.append(', '.join(w))
        s.append('from t0')
        query.extend(s)

        r = range(frame.shape[1])
        for i, j in zip(r[:-1], r[1:]):
            query.append(f'join t{j} on t{i}.rownum = t{j}.rownum')

        msg = ' '.join(query)
        return connection.execute(msg)

    @classmethod
    def _connection_to_frame(cls,
            *,
            connection: 'DuckDBPyConnection',
            ) -> TFrameAny:
        from static_frame.core.frame import Frame
        return Frame()



