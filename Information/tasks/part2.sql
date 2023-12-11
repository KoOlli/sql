-- Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE add_p2p_review(checked_peer varchar, checking_peer varchar, task_name varchar, status check_status) AS $$ 
  BEGIN 
    IF (status = 'Start') THEN
      INSERT INTO Checks
      VALUES((SELECT max(id) + 1 FROM Checks), checked_peer, (Select title from Tasks where title = task_name), current_date);
      INSERT INTO p2p
      VALUES((SELECT max(id) + 1 FROM p2p), (SELECT max(id) FROM Checks), checking_peer, status, current_time(0));
    ELSE
      INSERT INTO p2p
      VALUES((SELECT max(id) + 1 FROM p2p), (SELECT p2p."Check" FROM p2p
                JOIN checks ON p2p."Check" = checks.id
                  AND state = 'Start'
                  AND checkingpeer = checking_peer
                  AND task = task_name
                  AND peer = checked_peer
                ORDER BY p2p.id DESC
                LIMIT 1),
              checking_peer, status, current_time(0));
    END IF;
  END
$$ language plpgsql;

-- -- { tests
-- -- Добавим p2p-проверки
-- CALL add_p2p_review('lcoon', 'cjarrahd', 'C2_string+', 'Start');
-- CALL add_p2p_review('lcoon', 'cjarrahd', 'C2_string+', 'Failure');
-- CALL add_p2p_review('cjarrahd', 'oshipwri', 'C2_string+', 'Start');
-- CALL add_p2p_review('cjarrahd', 'oshipwri', 'C2_string+', 'Success');
-- CALL add_p2p_review('fscourge', 'lcoon', 'C3_SimpleBashUtils', 'Start');
-- CALL add_p2p_review('fscourge', 'lcoon', 'C3_SimpleBashUtils', 'Success');
-- -- } tests


-- Написать процедуру добавления проверки Verter'ом

CREATE OR REPLACE PROCEDURE add_verter_review(checking_peer varchar, task_name varchar, status check_status) AS $$ 
  BEGIN 
    INSERT INTO Verter
    VALUES((SELECT max(id) + 1 FROM Verter), (SELECT "Check" FROM p2p
      JOIN checks ON p2p."Check" = checks.id
        AND peer = checking_peer
        AND task = task_name
        AND p2p.state = 'Success'
      ORDER BY p2p.id DESC
      LIMIT 1), status, current_time(0));
  END
$$ language plpgsql;

-- -- { tests
-- -- Добавим Verter-проверки
-- CALL add_verter_review('fscourge', 'C5_decimal', 'Success', '16:33:00');
-- CALL add_verter_review('bsuper', 'C6_matrix', 'Success', '12:30:00');
-- CALL add_verter_review('oshipwri', 'C2_string+', 'Success', '19:58:00');
-- CALL add_verter_review('cjarrahd', 'C3_SimpleBashUtils', 'Success', '21:26:00');
-- -- } tests


-- Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints

CREATE or replace FUNCTION for_trigger_add_points() RETURNS TRIGGER AS $$
begin 
  update TransferredPoints
  set pointsamount = pointsamount + 1
  where checkingpeer = NEW.checkingpeer
  and checkedpeer = (SELECT peer from checks join p2p on checks.id = p2p."Check" where p2p."Check" = NEW."Check");
  return null;
end
$$ language plpgsql;

create or replace trigger trigger_add_points
after insert on p2p
for each row when (NEW.state = 'Start')
execute function for_trigger_add_points();

-- -- { tests
-- -- Изменим количество поинтов у пиров
-- CALL add_p2p_review('fscourge', 'oshipwri', 'C2_string+', 'Start');
-- CALL add_p2p_review('oshipwri', 'cjarrahd', 'C3_SimpleBashUtils', 'Start');
-- CALL add_p2p_review('cjarrahd', 'lcoon', 'C4_math', 'Start');
-- CALL add_p2p_review('bsuper', 'fscourge', 'C5_decimal', 'Start');
-- -- } tests


-- Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
create or replace function for_trigger_add_xp() returns trigger as $$
begin
  if (select tasks.MaxXP > NEW.XPAmount from tasks inner join checks on tasks.title = checks.task where checks.id = NEW."Check") THEN
    return null;
  end if;
  if (select count(*) = 0 from checks
        inner join p2p on checks.id = p2p."Check"
        left join verter on checks.id = verter."Check"
        where checks.id = NEW."Check"
          and p2p.state = 'Success') then
      return null;
  end if;
  return NEW;
end
$$ language plpgsql;

create or replace trigger trigger_add_xp
before insert 
on xp
for each row
execute function for_trigger_add_xp();

-- -- { tests
-- -- Добавим записи в таблицу ХР
-- INSERT INTO XP
-- VALUES((SELECT max(id) + 1 FROM xp), 10, '250');
-- INSERT INTO XP
-- VALUES((SELECT max(id) + 1 FROM xp), 9, '300');
-- INSERT INTO XP
-- VALUES((SELECT max(id) + 1 FROM xp), 8, '500');
-- -- } tests
