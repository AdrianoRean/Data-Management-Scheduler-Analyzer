from parallel_checker import parallel_checker
from sequential_checker import sequential_checker
from copy import deepcopy
import time
import json
from schedules import *

def calculate_averages(time_results, experiments, save = False):
    averages = {}
    best_checker_per_schedule = {}
    time_for_schedule = {}
    
    for checker, categories in time_results.items():
        index = 0
        averages[checker] = {}
        time_for_schedule[checker] = {}
        averages[checker]['overall'] = 0
        
        for category, times in categories.items():
            if times:
                avg_time = sum(delta for _, delta in times) / (experiments * len(times))
            else:
                avg_time = 0
            
            averages[checker][category] = avg_time
            
            for i, delta in times:
                time_for_schedule[checker][index] = (delta/experiments, category)
                if index not in best_checker_per_schedule or time_for_schedule[checker][index][0] < best_checker_per_schedule[index][1]:
                    best_checker_per_schedule[index] = (checker, time_for_schedule[checker][index][0], category)
                index += 1
        
            # Calculate overall average
            averages[checker]['overall'] += avg_time
    
    if save:
        with open("results.json", "w") as file:    
            results = {
                "Experiments" : experiments,
                "Categories" : averages,
                "Schedules" : time_for_schedule
            }  
            json.dump(results, file, indent=4)
    
    return averages, best_checker_per_schedule

def print_results(averages, best_checkers, experiments):
    with open("result.txt", "w") as file:
        line = f"Results averaged on {experiments} runs\n\n"
        print(line)
        file.write(line + "\n")
            
        for schedule, result in best_checkers.items():
            line = f"For schedule {schedule} which is {result[2]} the best checker is: {result[0]}"
            print(line)
            file.write(line + "\n")
        
        line = "\n##############################\n"
        print(line)
        file.write(line + "\n")
            
        average_results = {
            "two_pl" : {},
            "conflict" : {},
            "view" : {},
            "none" : {},
            "overall" : {}
        }
        for checker, results in averages.items():
            for category, result in results.items():
                average_results[category][checker] = result
        
        for category, results in average_results.items():
            line = f"Results for {category}"
            print(line)
            file.write(line + "\n")
            checkers = list(results.keys())
            line = f"For {checkers[0]}, {checkers[1]} checkers: {results[checkers[0]]} --- {results[checkers[1]]}."
            print(line)
            file.write(line + "\n")
            if results[checkers[0]] < results[checkers[1]]:
                line = f"Hence winner is {checkers[0]}"
            elif results[checkers[0]] > results[checkers[1]]:
                line = f"Hence winner is {checkers[1]}"
            else:
                line = f"No winner"
            line = line + "\n*************************"
            print(line)
            file.write(line + "\n")
        
        line = "##############################"
        print(line)
        file.write(line + "\n")
        
def run_experiments(experiments, save = False):
    checkers = [("sequential_checker", sequential_checker), ("parallel_checker", parallel_checker)]
    time_results = {
        "parallel_checker" : {
            "two_pl" : [[index, 0] for index, _ in enumerate(two_pl_schedules)],
            "conflict" : [[index, 0] for index, _ in enumerate(conflict_schedules)],
            "view" : [[index, 0] for index, _ in enumerate(view_schedules)],
            "none" : [[index, 0] for index, _ in enumerate(none_schedule)]
            },
        "sequential_checker" : {
            "two_pl" : [[index, 0] for index, _ in enumerate(two_pl_schedules)],
            "conflict" : [[index, 0] for index, _ in enumerate(conflict_schedules)],
            "view" : [[index, 0] for index, _ in enumerate(view_schedules)],
            "none" : [[index, 0] for index, _ in enumerate(none_schedule)]
            }
    }
    #schedules = schedules[-3:-2]
    for i in range(0, experiments):
        for category_schedules in [two_pl_schedules, conflict_schedules, view_schedules, none_schedule]:
            for index, schedule in enumerate(category_schedules):
                for checker in checkers:
                    s = deepcopy(schedule)
                    start_time = time.perf_counter() # Avvia il timer
                    result = checker[1](s)
                    end_time = time.perf_counter()  # Ferma il timer 
                    delta = (end_time-start_time)
                    time_results[checker[0]][result][index][1] += delta
                    
                    #print(f"Checker: {checker[0]}\nSchedule: {index}\nResult: {result}\nElapsed time: {delta}\n*****************")
            
    averages, best_checkers = calculate_averages(time_results, experiments, save)
    
    print_results(averages, best_checkers, experiments)
        
if __name__ == "__main__":
    run_experiments(10000, True)