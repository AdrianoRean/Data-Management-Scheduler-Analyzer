from parallel_checker import parallel_checker
from sequential_checker import sequential_checker
from copy import deepcopy
import time
from schedules import *

def calculate_averages(time_results, experiments):
    averages = {}
    best_checker_per_schedule = {}
    
    for checker, categories in time_results.items():
        index = 0
        averages[checker] = {}
        total_time = 0
        total_count = 0
        
        for category, times in categories.items():
            if times:
                avg_time = sum(delta/experiments for _, delta in times) / len(times)
            else:
                avg_time = 0
            
            averages[checker][category] = avg_time
            total_time += sum(delta for _, delta in times)/experiments
            total_count += len(times)
            
            for i, delta in times:
                if index not in best_checker_per_schedule or delta/experiments < best_checker_per_schedule[i][1]:
                    best_checker_per_schedule[index] = (checker, delta/experiments, category)
                index += 1
        
        # Calculate overall average
        averages[checker]['overall'] = total_time / total_count if total_count > 0 else 0
    
    return averages, best_checker_per_schedule

def print_results(averages, best_checkers, experiments):
    print(f"Results averaged on {experiments} runs\n\n")
        
    for schedule, result in best_checkers.items():
        print(f"For schedule {schedule} which is {result[2]} the best checker is: {result[0]}")
        
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
        print(f"Results for {category}")
        checkers = list(results.keys())
        print(f"For {checkers[0]}, {checkers[1]} checkers: {results[checkers[0]]} --- {results[checkers[1]]}.")
        if results[checkers[0]] < results[checkers[1]]:
            print(f"Hence winner is {checkers[0]}")
        elif results[checkers[0]] > results[checkers[1]]:
            print(f"Hence winner is {checkers[1]}")
        else:
            print(f"No winner")
        print("*************************")
    print("######################")
        
def run_experiments(experiments):
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
            
    averages, best_checkers = calculate_averages(time_results, experiments)
    
    print_results(averages, best_checkers, experiments)
        
if __name__ == "__main__":
    run_experiments(10000)