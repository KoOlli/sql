create table Peers (
    Nickname varchar primary key,
    Birthday date not null
);

insert into Peers(Nickname, Birthday)
values('fscourge', '1986-08-03'),
      ('oshipwri', '1994-12-24'),
      ('cjarrahd', '1990-04-23'),
      ('bsuper', '2001-02-08'),
      ('lcoon', '2000-11-28');

create table Tasks (
    Title varchar primary key,
    ParentTask varchar,
    MaxXP integer,
    constraint fk_Tasks foreign key (ParentTask) references Tasks(Title)
);

insert into Tasks (Title, MaxXP)
values ('C2_string+', 500);

insert into Tasks (Title, ParentTask, MaxXP)
values ('C3_SimpleBashUtils', 'C2_string+', 250),
       ('C5_decimal', 'C3_SimpleBashUtils', 350),
       ('C4_math', 'C3_SimpleBashUtils', 300),
       ('C6_matrix', 'C5_decimal', 200);

create type check_status as enum ('Start', 'Success', 'Failure');

create table Checks (
    ID serial primary key,
    Peer varchar not null,
    Task varchar not null,
    "Date" date not null,
    constraint  fk_Checks_Peer foreign key (Peer) references Peers(Nickname),
    constraint  fk_Checks_Task foreign key  (Task) references Tasks(Title)
);

insert into Checks(Peer, Task, "Date")
values ('fscourge', 'C2_string+', '2021-12-06'),
       ('oshipwri', 'C3_SimpleBashUtils', '2021-12-24'),
       ('cjarrahd', 'C4_math', '2022-01-28'),
       ('bsuper', 'C5_decimal', '2022-02-08'),
       ('lcoon', 'C6_matrix', '2022-03-20');

create table P2P (
    ID serial primary key,
    "Check" bigint not null,
    CheckingPeer varchar not null,
    State check_status,
    "Time" time default current_time,
    constraint fk_P2P_Check foreign key ("Check") references Checks(ID),
    constraint  fk_P2P_CheckingPeer foreign key (CheckingPeer) references Peers(Nickname)
);

insert into P2P("Check", CheckingPeer, State, "Time")
values (1, 'oshipwri', 'Start', '19:22'),
       (1, 'oshipwri', 'Success', '19:58'),
       (2, 'cjarrahd', 'Start', '21:02'),
       (2, 'cjarrahd', 'Success', '21:26'),
       (3, 'lcoon', 'Start', '22:15'),
       (3, 'lcoon', 'Failure', '22:43'),
       (4, 'fscourge', 'Start', '15:54'),
       (4, 'fscourge', 'Success', '16:33'),
       (5, 'bsuper', 'Start', '12:12'),
       (5, 'bsuper', 'Success', '12:30');

create table Verter (
    ID serial primary key,
    "Check" bigint,
    State check_status,
    "Time" time default current_time,
    constraint fk_Verter_Check foreign key ("Check") references Checks(ID)
);

insert into Verter("Check", State, "Time")
values (1, 'Start', '20:00'),
       (1, 'Success', '20:12'),
       (2, 'Start', '21:30'),
       (2, 'Success', '21:45'),
       (4, 'Start', '16:37'),
       (4, 'Failure', '16:45'),
       (5, 'Start', '12:33'),
       (5, 'Success', '12:40');

create table TransferredPoints (
    ID serial primary key,
    CheckingPeer varchar not null,
    CheckedPeer varchar not null,
    PointsAmount bigint default 0,
    constraint  fk_TransferedPoints_CheckingPeer foreign key (CheckingPeer) references Peers(Nickname),
    constraint  fk_TransferedPoints_CheckedPeer foreign key (CheckedPeer) references Peers(Nickname)
);

with  get_peers_name as (select distinct CheckingPeer as checking, C.Peer as checked, count(distinct "Check") as counter
                         from P2P
                             join Checks C on C.ID = P2P."Check"
                         group by checking, checked)
insert into TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount)
select checking, checked, counter
from get_peers_name;

create table Friends (
    ID serial primary key,
    Peer1 varchar not null,
    Peer2 varchar not null,
    constraint  fk_Friends_Peer1 foreign key (Peer1) references Peers(Nickname),
    constraint  fk_Friends_Peer2 foreign key (Peer2) references Peers(Nickname)
);

insert into Friends(Peer1, Peer2)
values ('fscourge', 'oshipwri'),
       ('oshipwri', 'fscourge'),
       ('bsuper', 'lcoon'),
       ('lcoon', 'bsuper'),
       ('cjarrahd', 'fscourge'),
       ('fscourge', 'cjarrahd'),
       ('oshipwri', 'cjarrahd'),
       ('cjarrahd', 'oshipwri');

create table Recommendations (
    ID serial primary key,
    Peer varchar not null,
    RecommendedPeer varchar not null,
    constraint  fk_Recommendations_Peer foreign key (Peer) references Peers(Nickname),
    constraint  fk_Recommendations_RecommendedPeer foreign key (RecommendedPeer) references Peers(Nickname)
);

with check_p2p as (select CheckingPeer as checking, "Check" as checked from P2P where State = 'Success')
insert into Recommendations(Peer, RecommendedPeer)
select C.Peer, checking
from Checks C join check_p2p on check_p2p.checked = C.ID;

create table XP (
    ID serial primary key,
    "Check" bigint,
    XPAmount integer default 0,
    constraint fk_XP_Check foreign key ("Check") references Checks(ID)
);

alter table XP
add constraint ch_xp_xpamount
  check (XPAmount >= 0);

with tmp_checks as (select Checks.ID as checks_id, Checks.Task as checks_task
                    from Checks
                        join P2P p on p."Check" = Checks.ID and p.State = 'Success'
                    except
                    select  Checks.ID as checks_id, Checks.Task as checks_task
                    from Checks
                        join Verter V on Checks.ID = V."Check" and V.State = 'Failure'),
    tmp_tasks_xp as (select distinct MaxXP as tasks_xp, Tasks.Title as tasks_title
                     from Tasks
                         join Checks C on Tasks.Title = C.Task
                         join P2P p on p."Check" = C.ID and p.State = 'Success')
insert into XP("Check", XPAmount)
select distinct checks_id, tasks_xp
from tmp_checks join tmp_tasks_xp on tasks_title = checks_task;

create table TimeTracking (
    ID serial primary key,
    Peer varchar not null,
    "Date" date not null,
    "Time" time default current_time,
    State integer not null,
    constraint fk_XP_Peer foreign key (Peer) references Peers(Nickname)
);

alter table TimeTracking
    add constraint ch_state check ( State between 1 and 2);

insert into TimeTracking(Peer, "Date", "Time", State)
values ('fscourge', '2021-11-28', '16:44', 1),
       ('fscourge', '2021-11-28', '21:12', 2),
       ('oshipwri', '2021-11-29', '12:15', 1),
       ('oshipwri', '2021-11-29', '13:21', 2),
       ('oshipwri', '2021-11-29', '14:55', 1),
       ('oshipwri', '2021-11-29', '19:32', 2),
       ('cjarrahd', '2021-12-01', '18:21', 1),
       ('cjarrahd', '2021-12-01', '23:42', 2),
       ('lcoon', '2021-12-02', '15:22', 1),
       ('bsuper', '2021-12-02', '15:23', 1),
       ('lcoon', '2021-12-02', '22:12', 2),
       ('bsuper', '2021-12-02', '22:13', 2);

create or replace procedure import_from_csv(directory text) as
$$
declare
    str text;
BEGIN
    str:='copy Peers(Nickname, Birthday) from ''' || directory || '/peers.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Tasks(Title, ParentTask, MaxXP) from ''' || directory || '/tasks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Checks(Peer, Task, "Date") from ''' || directory || '/checks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy P2P("Check", CheckingPeer, State, "Time") from ''' || directory || '/p2p.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Verter("Check", State, "Time") from ''' || directory || '/verter.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount) from ''' || directory || '/transferred_points.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Friends(Peer1, Peer2) from ''' || directory || '/friends.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Recommendations(Peer, RecommendedPeer) from ''' || directory || '/recommendations.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy XP("Check", XPAmount) from ''' || directory || '/xp.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy TimeTracking(Peer, "Date", "Time", State) from ''' || directory || '/time_tracking.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
END;
$$
    language plpgsql;

call import_from_csv('c:\21-school\SQL2_Info21_v1.0-2\src\csv');

create or replace procedure export_to_csv(directory text) as
$$
declare
    str text;
BEGIN
    str:='copy Peers(Nickname, Birthday) to ''' || directory || '/peers.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Tasks(Title, ParentTask, MaxXP) to ''' || directory || '/tasks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Checks(Peer, Task, "Date") to ''' || directory || '/checks.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy P2P("Check", CheckingPeer, State, "Time") to ''' || directory || '/p2p.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Verter("Check", State, "Time") to ''' || directory || '/verter.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy TransferredPoints(CheckingPeer, CheckedPeer, PointsAmount) to ''' || directory || '/transferred_points.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Friends(Peer1, Peer2) to ''' || directory || '/friends.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy Recommendations(Peer, RecommendedPeer) to ''' || directory || '/recommendations.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy XP("Check", XPAmount) to ''' || directory || '/xp.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
    str:='copy TimeTracking(Peer, "Date", "Time", State) to ''' || directory || '/time_tracking.csv'' DELIMITER '','' CSV HEADER';
    EXECUTE (str);
END;
$$
    language plpgsql;

call export_to_csv('c:\21-school\SQL2_Info21_v1.0-2\src\csv');

-- DROP TABLE IF EXISTS P2P CASCADE;
-- DROP TABLE IF EXISTS Checks CASCADE;
-- DROP TABLE IF EXISTS TimeTracking CASCADE;
-- DROP TABLE IF EXISTS TransferredPoints CASCADE;
-- DROP TABLE IF EXISTS Verter CASCADE;
-- DROP TABLE IF EXISTS Friends CASCADE;
-- DROP TABLE IF EXISTS Peers CASCADE;
-- DROP TABLE IF EXISTS Recommendations CASCADE;
-- DROP TABLE IF EXISTS Tasks CASCADE;
-- DROP TABLE IF EXISTS XP CASCADE;
-- DROP ROUTINE IF EXISTS import_from_csv(directory text);
-- DROP ROUTINE IF EXISTS export_to_csv(directory text);
-- drop type check_status cascade;



