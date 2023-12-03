--columns where deaths have happened
select *
from CovidDeaths
where continent is not null
order by 3,4

--columns ordered as per the location, then as per the dates
select location, date, total_cases, new_cases, total_deaths
from coviddeaths
order by 1,2

--checking the death rate in india
select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_rate
from coviddeaths
where location like 'india'
order by 2

--checking the max death rate in india and when
select top 1 location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_rate
from coviddeaths
where location like 'india'
order by 5 desc

--checking the min death rate in india and when
select top 1 location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_rate
from coviddeaths
where location like 'india' and total_cases is not null and total_deaths is not null
order by 5

--total cases vs population (infection rate)
select location, date, total_cases, population, (total_cases/population)*100 as infection_rate
from coviddeaths
where location like 'india'
order by 2

--countries with highest infection rate compared to population
select location, population,  max((total_cases/population)*100) as infectionrate
from CovidDeaths
group by location, population
order by 3 desc

--countries with highest death rate per population
select location, max(cast(total_deaths as int)) as total_death_count
from coviddeaths
where continent is not null
group by location
order by 2 desc

--day with maximum number of deaths in each country
select cd.location,  max(cast(new_deaths as int)) as new_deaths, date
from CovidDeaths cd
join (select location, max(cast(new_deaths as int))  as max_deaths
from coviddeaths
group by location) as new_table
on new_table.location=cd.location and cd.new_deaths=new_table.max_deaths
where continent is not null
group by cd.location, date
order by 2 desc

--new deaths in India
select location, cast(new_deaths as int) new_deaths_india
from CovidDeaths
where location='India'
order by 2 desc


--breaking down by continent with highest death count
select location, max(cast (total_deaths as int)) as total_death_per_continent
from coviddeaths
where continent is null
group by location
order by 2 desc

--new deaths everyday, throughout the world
select distinct date, sum(new_cases) over (order by date) as total_cases
from CovidDeaths
where continent is not null
order by 1

--total number of cases on each day across the world and death percentage each day across the world
select date, sum(new_cases) as total_cases, sum(cast (new_deaths as int)) as total_deaths, sum(cast (new_deaths as int))/ sum(new_cases)*100 as world_death_pct
from coviddeaths
where continent is not null 
group by date
order by sum(new_cases), date

--total cases with death pct across the world
select sum(new_cases) as total_cases, sum(cast (new_deaths as int)) as total_deaths, sum(cast (new_deaths as int))/ sum(new_cases)*100 as world_death_pct
from coviddeaths
where continent is not null 
order by sum(new_cases)

--use of case statement
select location, date, new_cases, population, (new_cases/population)*100 as infection_rate,
case
when (new_cases/population)*100>0.05 then 'danger+'
else 'danger'
end
from coviddeaths
order by 1,2

--total new vaccinations in each location, besides its population
select dea.continent, dea.location,dea.date,dea.population,vac.new_vaccinations,
sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,dea.date) as rolling_vac
from coviddeaths dea 
join CovidVaccinations vac
on dea.location=vac.location
and dea.date=vac.date
where dea.continent is not null
order by 2,3

--creating a cte
with popvsvac (continent,location,date,population,new_vaccinations,rolling_vac)
as (
select dea.continent, dea.location,dea.date,dea.population,vac.new_vaccinations,
sum(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,dea.date) as rolling_vac
from coviddeaths dea 
join CovidVaccinations vac
on dea.location=vac.location
and dea.date=vac.date
where dea.continent is not null)
select *, (rolling_vac/population)*100 as pct_rolling
from popvsvac

--ranking locations as per their populations
--using rank and dense rank functions
select distinct location, population,
rank () over(order by population desc) rnk,
dense_rank () over(order by population desc) drnk
from CovidDeaths
where continent is not null
order by 2 desc

--ranking locations as per total deaths
select distinct location, cast(total_deaths as int) total_deaths, 
rank() over (partition by location order by cast(total_deaths as int)) rnk
from CovidDeaths
where continent is not null
order by 1,3

--average number of new cases over a rolling seven-day period for each location 
select distinct location,date, new_cases,
avg(new_cases) over (partition by location order by date rows between 6 preceding and 0 following) avg
from CovidDeaths
where continent is not null and new_cases is not null
order by 1,2

--rolling number of new cases in each location, compared with total_cases, to check accuracy
select location, date, new_cases,
sum(new_cases) over (partition by location order by date rows between unbounded preceding and 0 following) rolling, total_cases
from CovidDeaths
where continent is not null and new_cases is not null
order by 1,2

--running sum of new cases throughout the world
select distinct date,
sum(new_cases) over (order by date) new_cases_world
from CovidDeaths
where continent is not null and new_cases is not null
order by 1

--use of first_value function, to finds number of deaths on the first day
select distinct location,date, 
first_value (cast(new_deaths as int)) over (partition by location order by date)
from coviddeaths
where continent is not null and (cast(new_deaths as int)) is not null
order by 1

--pct of new deaths in each location
select location, date, new_deaths, (cast(new_deaths as float)/total_death)*100 pct
from (select location, date, new_deaths,
sum((cast(new_deaths as int))) over (partition by location ) total_death
from CovidDeaths
where continent is not null and (cast(new_deaths as int)) is not null) a

--countries where deaths increased the follwoing day
select location, count(diff) inc 
from
(select location, date, new_deaths, 
lag((cast(new_deaths as int))) over (partition by location order by date) previous_day, 
cast(new_deaths as int)- lag((cast(new_deaths as int))) over (partition by location order by date) diff
from CovidDeaths
where continent is not null and (cast(new_deaths as int)) is not null) a
where diff < 0
group by location 

--countries with increasing number of cases for three consecutive days
select location, sum(y_n) inc
from
(select location, date, prv_day, curr_day, nxt_day,
case when prv_day<curr_day and curr_day<nxt_day then 1 else 0 end y_n
from
(select location, date, 
lag(cast(new_deaths as int)) over (partition by location order by date) prv_day,
new_deaths as curr_day,
lead(cast(new_deaths as int)) over (partition by location order by date) nxt_day
from CovidDeaths
where continent is not null and (cast(new_deaths as int)) is not null) a) b
group by location 

--which country had maximum cases in each month of each year
select location, yr, month, total_monthly_cases, max_c
from 
(select location, yr, month, total_monthly_cases,
(max(total_monthly_cases) over (order by yr, month)) max_c
from
(select location, sum(new_cases) total_monthly_cases, datename(year,date) yr , datepart(month,date) month, datename(month,date) mnth
from CovidDeaths
where continent is not null and (cast(new_deaths as int)) is not null
group by location, datename(year,date), datepart(month,date), datename(month,date)
) a)b
where max_c=total_monthly_cases

--sum of cases in each location, monthlwise per year
select location, c.yr, c.mnth, month, sumnn
from 
(select location, datepart(year,date) yr, datepart(month,date) mnth, datename(month,date) month, sum(new_cases) sumn
from CovidDeaths
group by location, datepart(year,date), datepart(month,date), datename(month,date) ) c
inner join
(select max(sumn) sumnn, yr, mnth
from
(select location, datepart(year,date) yr, datepart(month,date) mnth, datename(month,date) month, sum(new_cases) sumn
from CovidDeaths
where continent is not null
group by location, datepart(year,date), datepart(month,date), datename(month,date) 
) a
group by yr, mnth) b
on c.yr=b.yr and c.mnth=b.mnth and c.sumn=b.sumnn
order by 2,3


