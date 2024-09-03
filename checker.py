def check_conflict_serializability(graph, n_transaction):
    visited = {}
    remaining = []
    for i in range(0,n_transaction):
        remaining.append(i)
        visited[i] = False
    
    actual_node = remaining.pop()
    visited[actual_node] = True
    
    while True:
        if check_cycle(graph, n_transaction, actual_node, visited, remaining):
            return False
        else:
            if remaining != []:
                actual_node = remaining.pop()
                visited[actual_node] = True
            else:
                return True
    
def check_cycle(graph, n_transaction, actual_node, visited, remaining):
    for next_node in range(0, n_transaction):
        if graph[actual_node][next_node] == True:
            if not visited[next_node]:
                visited[next_node] = True
                remaining.remove(next_node)
                if check_cycle(graph, n_transaction, next_node, visited, remaining):
                    return True
            else:
                return True
    return False

def check_view_serializabilty(read_from, final_write, serial_schedules):
    for schedule in serial_schedules:
        print(schedule)
        if read_from == schedule["read_from"] and final_write == schedule["final_write"]:
            return True
    else:
        return False