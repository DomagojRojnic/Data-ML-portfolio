-- 1. How many Olympic games have taken place?
select count(distinct(year))
from athlete_events ae

-- 2. List all the Olympic games along with the cities where they were held.
select distinct(games), city
from athlete_events ae
order by 1

--3. List every region that took part in the 1896 Summer Games.
select distinct(nr.region) 
from athlete_events ae
inner join noc_regions nr on ae.noc = nr.noc
where games = '1896 Summer'

--4. Mention the total number of countries that competed in each Olympic game.
select games, count(distinct(team)) as "No. of countries competed"
from athlete_events ae
group by 1
order by 2

--5. Which Olympic game has the most and least countries participating?
(select games, count(distinct(team)) as "No. of countries competed"
from athlete_events ae
group by 1
order by 2 desc
limit 1)
union all
(select games, count(distinct(team)) as "No. of countries competed"
from athlete_events ae
group by 1
order by 2 asc
limit 1)

-- 6. Which nation or country has competed in each and every Olympic Game?
with country_competition_count as 
(
	select nr.region, count(distinct(games)) as "No. of competitions"
	from athlete_events ae
	inner join noc_regions nr on ae.noc = nr.noc
	group by 1
	order by 2 desc
)
select *
from country_competition_count
where "No. of competitions" = (select count(distinct(games)) from athlete_events)

-- 7. Get the top 5 medal-winning nations from the Olympics. Success is measured by the total number of medals earned.
select nr.region, count(ae.medal)
from athlete_events ae
inner join noc_regions nr on ae.noc = nr.noc 
group by 1
order by 2 desc
limit 5

-- 8. How many gold, silver, and bronze medals did each nation win in total?
select nr.region , 
	sum(case when medal = 'Gold' then 1 else 0 end) as "gold_count",
	sum(case when medal = 'Silver' then 1 else 0 end) as "silver_count",
	sum(case when medal = 'Bronze' then 1 else 0 end) as "bronze_count"
from athlete_events ae
left join noc_regions nr on ae.noc = nr.noc
group by 1
order by 2 desc

-- 9. List all of Nigeria’s gold, silver, and bronze medals from each Olympic game.
with t1 as
(
select games,
	sum(case when medal = 'Gold' then 1 else 0 end) as "gold_count",
	sum(case when medal = 'Silver' then 1 else 0 end) as "silver_count",
	sum(case when medal = 'Bronze' then 1 else 0 end) as "bronze_count"
from athlete_events ae
where noc = 'NIG'
group by 1
)

select distinct(ae.games), t1.gold_count, t1.silver_count, t1.bronze_count
from athlete_events ae
left join t1 on ae.games = t1.games
order by 1


--10. List the total number of gold, silver, and bronze medals each nation has earned at each Olympic game.
select ae.games, nr.region,
	sum(case when medal = 'Gold' then 1 else 0 end) as "gold_count",
	sum(case when medal = 'Silver' then 1 else 0 end) as "silver_count",
	sum(case when medal = 'Bronze' then 1 else 0 end) as "bronze_count"
from athlete_events ae
inner join noc_regions nr on ae.noc = nr.noc
group by 1, 2
order by 1, 2

-- 11. Which country has never won a gold medal but has instead received a silver or bronze?
with t1 as
(
	select nr.region,
		sum(case when medal = 'Gold' then 1 else 0 end) as "gold_count",
		sum(case when medal = 'Silver' then 1 else 0 end) as "silver_count",
		sum(case when medal = 'Bronze' then 1 else 0 end) as "bronze_count"
	from athlete_events ae
	inner join noc_regions nr on ae.noc = nr.noc
	group by 1
	order by 1
)

select *
from t1
where t1."gold_count" = 0 and (t1."silver_count" > 0 or t1."bronze_count" > 0)

-- 12. Which countries never won a single medal?
with t1 as
(
	select nr.region,
		sum(case when medal = 'Gold' then 1 else 0 end) as "gold_count",
		sum(case when medal = 'Silver' then 1 else 0 end) as "silver_count",
		sum(case when medal = 'Bronze' then 1 else 0 end) as "bronze_count"
	from athlete_events ae
	inner join noc_regions nr on ae.noc = nr.noc
	group by 1
	order by 1
)

select t1.region
from t1
where t1."gold_count" = 0 and t1."silver_count" = 0 and t1."bronze_count" = 0

-- 13. Mention the total no of nations who participated in each olympics game?
with t1 as
(
	select distinct(noc), count(distinct(games)) as "total_games"
	from athlete_events ae
	group by 1
	order by 2 desc
)

select count(noc)
from t1
where "total_games" = (select count(distinct games) from athlete_events ae)

-- 14. Which nation has participated in all of the olympic games?
select nr.region, count(distinct(games)) as "no_of_participaitons"
from athlete_events ae
inner join noc_regions nr on ae.noc = nr.noc
group by 1
having count(distinct(games)) = (select count(distinct games) from athlete_events ae2)

-- 15. Which year saw the highest and lowest no of countries participating in olympics?
with t1 as
(
	select distinct(ae."year"), count(distinct(noc)) as "total_countries"
	from athlete_events ae
	group by 1
)
(select *
from t1
order by 2 desc
limit 1)
union all
(select *
from t1
order by 2
limit 1)

-- 16. Fetch the total no of sports played in each olympic games.
select games, count(distinct(sport)) as "total_no_of_sports_played"
from athlete_events ae
group by 1
order by 2 desc

-- 17. Fetch details of the oldest athletes to win a gold medal.
select *
from (
	select *, ae."name", ae."age", dense_rank() over (order by age desc) rank
	from athlete_events ae
	where medal = 'Gold' and ae."age" is not null
)
where rank = 1

-- 18. Find the Ratio of male and female athletes participated in all olympic games.
with t1 as
(
	select games, count(distinct(name)) as "female_count"
	from athlete_events ae
	where sex = 'F'
	group by 1
	order by 2 desc
),
t2 as
(
	select games, count(distinct(name)) as "male_count"
	from athlete_events ae
	where sex = 'M'
	group by 1
	order by 2 desc
)

select t1.games, (cast(t2."male_count" as decimal) / (t1."female_count")) as "ratio"
from t1
inner join t2 on t1.games = t2.games
order by 1

-- 19. Fetch the top 5 athletes who have won the most gold medals.
select ae."name", count(medal)
from athlete_events ae
where medal = 'Gold'
group by 1
order by 2 desc
limit 5

-- 20. Fetch the top 5 athletes who have won the most medals.
select ae."name", count(medal)
from athlete_events ae
where medal is not null
group by 1
order by 2 desc
limit 5
