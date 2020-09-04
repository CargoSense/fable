defmodule Fable.Migrations do
  use Ecto.Migration

  def events_table(table) do
    create table(table, primary_key: false) do
      add(:id, :bigserial, primary_key: true)
      add(:prev_event_id, :integer)
      add(:aggregate_id, :uuid, null: false)
      add(:aggregate_table, :string, null: false)
      add(:type, :string, null: false)
      add(:version, :integer, null: false)
      add(:meta, :jsonb)
      add(:data, :jsonb)
      add(:inserted_at, :timestamp, null: false, default: fragment("statement_timestamp()"))
    end

    create index(table, [:aggregate_id, :aggregate_table])

    execute(
      """
      create or replace function fn_trigger_last_event_update(_tbl regclass) returns trigger
      security definer
      language plpgsql
      as $$
        DECLARE
          rcount int;
          result uuid;
          find_aggregate text := format(
            'SELECT id FROM %I WHERE id = $1', NEW.aggregate_table
          );
          event_json text := json_build_object(
            'aggregate_id', NEW.aggregate_id,
            'aggregate_table', NEW.aggregate_table,
            'events_table', _tbl,
            'id', NEW.id
          );
          update_aggregate text := format(
            'UPDATE %I SET last_event_id = $1 WHERE id = $2 AND (last_event_id = $3 OR last_event_id IS NULL) RETURNING id',
            NEW.aggregate_table);
        BEGIN
          EXECUTE update_aggregate
            USING NEW.id, NEW.aggregate_id, NEW.prev_event_id
            INTO STRICT result;
            PERFORM pg_notify('events', event_json);
            RETURN NEW;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              EXECUTE find_aggregate USING NEW.aggregate_id;
              get diagnostics rcount = ROW_COUNT;
              IF rcount > 0 THEN
                RAISE EXCEPTION 'Aggregate version out of date: % %',
                  NEW.aggregate_table, NEW.aggregate_id
                  USING ERRCODE = 'serialization_failure';
                RETURN NEW;
              ELSE
                PERFORM pg_notify('events', event_json);
                RETURN NEW;
              END IF;
        END;
      $$
      """,
      "drop function fn_trigger_last_event_update"
    )

    execute(
      """
      create trigger event_insert_update_last_event_id after insert on #{table}
      for each row
        execute procedure fn_trigger_last_event_update(#{table})
      """,
      "drop trigger event_insert_update_last_event_id ON #{table}"
    )
  end
end
