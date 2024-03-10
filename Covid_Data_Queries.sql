-- The dataset is from https://ourworldindata.org/covid-deaths

-- CovidDeaths AS CD
-- CovidVaccinations AS CV

SELECT * FROM CovidDeaths
SELECT * FROM CovidVaccinations

-- SELECT most useful data
SELECT continent, location, date, population, total_cases, new_cases, total_deaths, new_deaths
FROM CovidDeaths
WHERE continent IS NOT NULL
ORDER BY continent, location, date;

-- Total cases vs total deaths
-- Likelihood of dying if you contract covid in Greece
SELECT location, date, total_cases, total_deaths, 
CAST(total_deaths AS FLOAT)/CAST(total_cases AS FLOAT)*100 AS mortality_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL AND total_cases IS NOT NULL 
AND location LIKE '%Greece%'
ORDER BY location, date;

-- Total cases vs total deaths
-- Likelihood of dying if you contract covid for European countries
SELECT location, date, total_cases, total_deaths, 
CAST(total_deaths AS FLOAT)/CAST(total_cases AS FLOAT)*100 AS mortality_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL AND total_cases IS NOT NULL 
AND continent LIKE '%Europe%'
ORDER BY location, date;

-- Total Cases vs Population
-- Shows what percentage of population has been infected with Covid in Greece
SELECT location, date, population, total_cases,
(CONVERT(FLOAT, total_cases)/population)*100 AS infection_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL AND total_cases IS NOT NULL AND location LIKE '%Greece%'
ORDER BY location, date;

-- Total Cases vs Population
-- Shows what percentage of population has been infected with Covid for European countries
SELECT location, date, population, total_cases,
(CONVERT(FLOAT, total_cases)/population)*100 AS infection_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL AND total_cases IS NOT NULL AND continent LIKE '%Europe%'
ORDER BY location, date;

-- Countries with Highest Infection Rate
SELECT location, AVG(population) AS population, MAX(CONVERT(FLOAT, total_cases)) AS total_cases, 
(MAX(CONVERT(FLOAT, total_cases))/AVG(population))*100 AS infection_rate
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY infection_rate DESC

-- Countries with Highest Death Count per Population
SELECT continent, location, AVG(population) AS population, MAX(CONVERT(FLOAT,total_deaths)) AS total_deaths,
(MAX(CONVERT(FLOAT,total_deaths))/AVG(population))*100 AS death_percentage
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent, location
ORDER BY death_percentage DESC;

-- Showing contintents with the highest death count
SELECT continent, SUM(new_deaths) AS death_count
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY death_count DESC;

-- GLOBAL NUMBERS (total cases, total deaths, mortality rate)
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, (SUM(new_deaths)/SUM(new_cases))*100 AS mortality_rate
FROM CovidDeaths
WHERE continent IS NOT NULL;

-- Total Population vs Vaccinations
-- Shows Percentage of population that have been vaccinated at least once in Greece
SELECT CD.location, CD.date, CD.population, CV.people_vaccinated, (CONVERT(FLOAT, CV.people_vaccinated)/CD.population)*100 AS vaccination_percentage
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location AND CD.date = CV.date
WHERE CD.continent IS NOT NULL AND CV.people_vaccinated IS NOT NULL AND CD.location LIKE '%Greece%'
ORDER BY location, date

-- Total Population vs Vaccinations
-- Shows Percentage of population that have been vaccinated at least once in European countries
SELECT CD.location, CD.date, CD.population, CV.people_vaccinated, (CONVERT(FLOAT, CV.people_vaccinated)/CD.population)*100 AS vaccination_percentage
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location AND CD.date = CV.date
WHERE CD.continent IS NOT NULL AND CV.people_vaccinated IS NOT NULL AND CD.continent LIKE '%Europe%'
ORDER BY location, date

-- Countries with highest vaccination percentage
SELECT CD.location, AVG(CD.population) AS population, MAX(CONVERT(FLOAT, CV.people_vaccinated)) people_vaccinated, 
100*MAX(CONVERT(FLOAT, CV.people_vaccinated))/AVG(CD.population) AS vaccination_percentage
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location AND CD.date = CV.date
WHERE CD.continent IS NOT NULL
GROUP BY CD.location
HAVING 100*MAX(CONVERT(FLOAT, CV.people_vaccinated))/AVG(CD.population) <= 100
ORDER BY vaccination_percentage DESC;

-- GLOBAL NUMBERS (covid tests and covid vaccinations)
SELECT SUM(CONVERT(FLOAT, new_tests)) AS total_tests, SUM(CONVERT(FLOAT, new_vaccinations)) AS total_vaccinations
FROM CovidVaccinations
WHERE continent IS NOT NULL;

-- Running total of covid tests in Greece
SELECT CD.location, CD.date, CD.population, CV.new_tests,
SUM(CONVERT(FLOAT, new_tests)) OVER(PARTITION BY CD.location ORDER BY CD.location, CD.date) AS total_tests
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location
AND CD.date = CV.date
WHERE CD.continent IS NOT NULL AND CD.location LIKE '%Greece%'
ORDER BY location, date

-- Running total of covid vaccinations in Greece using CTE
WITH RunningTotalVaccinations AS (
SELECT CD.location, CD.date, CD.population, CV.new_vaccinations,
SUM(CONVERT(FLOAT, CV.new_vaccinations)) OVER (PARTITION BY CD.location ORDER BY CD.location, CD.date) AS total_vaccinations
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location
AND CD.date = CV.date
WHERE CD.continent IS NOT NULL AND CD.location LIKE '%Greece%')
SELECT *, (total_vaccinations/population) AS vaccination_population_ratio
FROM RunningTotalVaccinations
ORDER BY location, date

-- Running total of covid vaccinations in Europe using temp table
DROP TABLE IF EXISTS #TempVaccinations

CREATE TABLE #TempVaccinations (
continent VARCHAR(100),
location VARCHAR(100),
date DATE,
population FLOAT,
new_vaccinations FLOAT,
total_vaccinations FLOAT
)

INSERT INTO #TempVaccinations
SELECT CD.continent, CD.location, CD.date, CD.population, CV.new_vaccinations,
SUM(CONVERT(FLOAT, CV.new_vaccinations)) OVER (PARTITION BY CD.location ORDER BY CD.location, CD.date) AS total_vaccinations
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location
AND CD.date = CV.date
WHERE CD.continent IS NOT NULL AND CD.continent LIKE '%Europe%'

SELECT *, total_vaccinations/population AS vaccination_population_ratio
FROM #TempVaccinations
ORDER BY location, date

-- Running total of covid vaccinations using view
DROP VIEW VaccinationsView

CREATE VIEW VaccinationsView AS
SELECT CD.location, CD.date, CD.population, CV.new_vaccinations,
SUM(CONVERT(FLOAT, CV.new_vaccinations)) OVER (PARTITION BY CD.location ORDER BY CD.location, CD.date) AS total_vaccinations
FROM CovidDeaths AS CD
JOIN CovidVaccinations AS CV
ON CD.location = CV.location
AND CD.date = CV.date
WHERE CD.continent IS NOT NULL

SELECT *, total_vaccinations/population AS vacc_pop_ratio
FROM VaccinationsView
WHERE location LIKE '%Greece%'
ORDER BY location, date

-- Running total of covid vaccinations with stored procedure
DROP PROCEDURE VaccinationsProcedure

CREATE PROCEDURE VaccinationsProcedure(@country AS VARCHAR(30))
AS
BEGIN
	SELECT CD.location, CD.date, CD.population, CV.new_vaccinations,
	SUM(CONVERT(FLOAT, CV.new_vaccinations)) OVER(PARTITION BY CD.location ORDER BY CD.location, CD.date) AS total_vaccinations,
	SUM(CONVERT(FLOAT, CV.new_vaccinations)) OVER(PARTITION BY CD.location ORDER BY CD.location, CD.date)/population AS vaccination_population_ratio
	FROM CovidDeaths AS CD
	JOIN CovidVaccinations AS CV
	ON CD.location = CV.location
	AND CD.date = CV.date
	WHERE CD.continent IS NOT NULL AND CD.location = @country
	ORDER BY CD.location, CD.date
END

EXEC VaccinationsProcedure
@country = 'Greece';