from copy import deepcopy

circuits = []

class Johnson:
    
    def __init__(self, circuits, n_transactions):
        self.circuits = circuits
        self.is_blind = False
        self.n_transactions = n_transactions

    def get_component_adiacency_list(connected_lists, s, v, n_transaction):
        new_lists = deepcopy(connected_lists)
        for i in range(0,s):
            new_lists.pop(i)
        for i in range(v, n_transaction):
            try:
                new_lists[i].remove(v)
            except:
                pass
        return new_lists

    def unblock(v, blocked, blocked_stack):
        blocked[v] = False
        to_del = []
        for w in blocked_stack[v]:
            to_del.append(w)
            unblock(w, blocked, blocked_stack)
        for w in to_del:
            blocked_stack[v].pop(w)
            
    def circuit(v, s, stack, blocked, blocked_stack, adiancency_list):
        #print("v: " + str(v) + ", s: " + str(s) + ", stack: " + str(stack) + ", blocked: " + str(blocked) + ", b_stack: " + str(blocked_stack) + ", adiacency_list: " + str(adiancency_list))
        f = False
        stack.append(v)
        blocked[v] = True
        for w in adiancency_list[v]:
            if w == s:
                circuits.append(deepcopy(stack))
                f = True
            elif not blocked[w]:
                f = circuit(w, s, stack, blocked, blocked_stack, adiancency_list)
            if f:
                unblock(v, blocked, blocked_stack)
            else:
                for w in adiancency_list[v]:
                    if v not in blocked_stack[w]:
                        blocked_stack[w].append(v)
            
            stack.remove(v)
            
            return f
        

    def johnson(connected_lists, n_transaction):
        l = -1
        
        for s in range (0, n_transaction - 1):
            adiancency_list = get_component_adiacency_list(connected_lists, s, l, n_transaction)
            stack = []
            blocked = [False for i in range(0, n_transaction)]
            blocked_stack = {}
            for i in range (s, n_transaction):
                blocked_stack[i] = []
            
            circuit(s, s, stack, blocked, blocked_stack, adiancency_list)
            l += 1
        
        return circuits
            
            
        