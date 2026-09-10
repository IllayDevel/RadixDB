---
title: Триггеры
description: Детерминированные row и statement triggers для изменений таблиц.
---

Trigger является catalog object, который связывает zero-argument `VOLATILE`
function с `RETURNS TRIGGER` и событие `INSERT`, `UPDATE` или `DELETE`. Он
работает в transaction вызвавшего statement и не может выполнить отдельный
commit.

## Определение

Сначала определите trigger function, затем прикрепите её к таблице:

```sql
CREATE FUNCTION docs_touch_trigger() RETURNS TRIGGER
LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;

CREATE TRIGGER docs_touch
BEFORE UPDATE OF value ON docs_trigger_rows
FOR EACH ROW PRIORITY 100
WHEN (OLD.value <> NEW.value)
EXECUTE FUNCTION docs_touch_trigger();
```

Event list может содержать `INSERT`, `UPDATE` и `DELETE`; `UPDATE OF` ограничивает
update trigger указанными столбцами. Timing имеет вид `BEFORE` или `AFTER`,
level: `FOR EACH ROW` или `FOR EACH STATEMENT`. Expression `WHEN` связывается с
target table при attachment trigger.

## OLD и NEW

Row trigger functions получают typed records, специализированные по target table:

| Event | `OLD` | `NEW` | Writable `NEW` |
| --- | --- | --- | --- |
| INSERT | недоступен | доступен | Только BEFORE ROW |
| UPDATE | доступен | доступен | Только BEFORE ROW |
| DELETE | доступен | недоступен | Никогда |

В procedural expressions используйте `NEW.column` и `OLD.column`. Static SQL
напрямую связывает record fields как `:NEW.column` и `:OLD.column`, включая
quoted column names. Binder специализирует type, nullability и event
availability при attachment. Whole-record SQL parameters не поддерживаются.
`OLD` всегда read-only. Statement triggers не могут обращаться к этим records.

BEFORE INSERT/UPDATE row trigger возвращает `NEW` для продолжения с возможно
изменённой row или `NULL` для suppression. BEFORE DELETE возвращает `OLD` или
подавляет строку через NULL. AFTER ROW и все statement triggers должны сделать
`RETURN NULL`. Suppressed row не входит в affected count и не вызывает
последующие row triggers, но statement triggers всё равно выполняются один раз.

## Порядок и видимость

Подходящие triggers выполняются по `(PRIORITY ASC, stable trigger object ID
ASC)`. Default priority равен `1000`, значение использует signed 32-bit range.
При равном priority порядок остаётся детерминированным по object identity.

Каждый BEFORE ROW trigger видит изменения предыдущего. `WHEN` вычисляется
непосредственно перед ним. AFTER ROW видит финальную сохранённую row. Statement
triggers выполняются один раз даже для statement с нулём изменённых строк.

## Ошибки и rollback

Trigger и его SQL leaves используют MVCC owner вызывающего statement. При
ошибке откатываются outer mutation и уже выполненные trigger effects:

```sql
CREATE FUNCTION docs_fail_trigger() RETURNS TRIGGER
LANGUAGE RADIX VOLATILE SECURITY INVOKER AS
BEGIN
    INSERT INTO docs_trigger_log VALUES (1);
    RAISE invalid_state('trigger failed');
END;
```

Admission definition отклоняет неверное использование OLD/NEW, ошибочный return
contract или несовместимый table descriptor до attachment trigger. Runtime
защищён active-chain guard и пределом trigger depth 32. Cycle возвращает
`PL_TRIGGER_CYCLE`, исчерпание depth: `PL_TRIGGER_DEPTH`; statement откатывается.

## Границы

Transition tables и deferred triggers в 1.2 отсутствуют. Trigger functions не
могут открывать транзакции, запускать jobs или обращаться к network, filesystem
и process API. Static dependency cycles отклоняются; dynamic SQL сохраняет
runtime guards и не обходит capabilities или privileges.

`CREATE OR REPLACE TRIGGER` обновляет совместимое definition. Attachment
удаляется по table-qualified identity:

```sql
DROP TRIGGER IF EXISTS docs_touch ON docs_trigger_rows RESTRICT;
```

`RESTRICT` является default и защищает static dependencies; явный `CASCADE`
атомарно удаляет dependent catalog objects.

Исполняемый пример документации проверяет изменение NEW и доказывает, что
raised trigger error не оставляет ни inserted row, ни trigger-log row. Принятый
executor suite дополнительно проверяет row suppression, все четыре hook
positions, deterministic order, cycles и rebuild после reopen.

Далее: [«Задания по расписанию»](../jobs/).
