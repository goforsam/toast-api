@echo off
REM Launch all 6 restaurant loads in parallel (Windows)

echo Starting parallel historical load for all 6 restaurants...
echo Each restaurant will load in a separate window
echo.

start "Restaurant 1" python load_by_restaurant.py 1
timeout /t 2 /nobreak >nul

start "Restaurant 2" python load_by_restaurant.py 2
timeout /t 2 /nobreak >nul

start "Restaurant 3" python load_by_restaurant.py 3
timeout /t 2 /nobreak >nul

start "Restaurant 4" python load_by_restaurant.py 4
timeout /t 2 /nobreak >nul

start "Restaurant 5" python load_by_restaurant.py 5
timeout /t 2 /nobreak >nul

start "Restaurant 6" python load_by_restaurant.py 6

echo.
echo All 6 restaurants started in parallel!
echo Check each window for progress.
echo This will take approximately 3-4 hours.
