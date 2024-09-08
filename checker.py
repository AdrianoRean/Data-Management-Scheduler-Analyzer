def check_conflict_serializability(graph, n_transaction):
    remaining = [i for i in range(0, n_transaction)]
    
    actual_node = remaining.pop()
    visited = [False for i in range(0, n_transaction)]
    visited[actual_node] = True
    
    while True:
        if check_cycle(graph, n_transaction, actual_node, remaining, visited):
            return False
        else:
            if remaining != []:
                actual_node = remaining.pop()
                visited = [False for i in range(0, n_transaction)]
                visited[actual_node] = True
            else:
                return True
    
def check_cycle(graph, n_transaction, actual_node, remaining, visited):
    for next_node in graph[actual_node]:
        if visited[next_node]:
            return True
        elif visited[next_node] in remaining:
            visited[next_node] = True
            remaining.remove(next_node)
            return check_cycle(graph, n_transaction, next_node, remaining, visited)
    return False

def check_view_serializabilty(read_from, final_write, serial_schedules):
    for schedule in serial_schedules:
        if read_from == serial_schedules[schedule]["read_from"] and final_write == serial_schedules[schedule]["final_write"]:
            return True
    else:
        return False