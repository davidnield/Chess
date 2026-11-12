"""Price, allocate and independently replay distinct study cards on a layered DAG."""
import numpy as np
from scipy.optimize import linprog
from scipy.sparse import coo_matrix, csr_matrix


class Allocator:
    def __init__(self,levels,weights):
        self.levels = levels
        self.hashes = sorted(set().union(*(set(l.ids) for l in levels)))
        self.index = {h:i for i,h in enumerate(self.hashes)}
        self.gids = [np.array([self.index[h] for h in l.ids],dtype=int) for l in levels]
        self.start = np.array([weights.get(h,0.) for h in levels[0].ids])
        self.slots,self.menus = [],{}
        self.max_moves = 0
        cap = self.start.copy()
        self.capacity = np.zeros(len(self.hashes))
        self.occurrence_cap = []
        for l,gids in zip(levels,self.gids):
            self.occurrence_cap.append(cap)
            self.capacity[gids] += cap
            counts = np.bincount(l.owners,minlength=len(l.ids))
            offsets = np.cumsum(counts)-counts
            slots = np.arange(len(l.owners))-offsets[l.owners]
            self.slots.append(slots)
            for i,h in enumerate(l.ids):
                menu = tuple(l.moves[offsets[i]:offsets[i]+counts[i]])
                if self.menus.setdefault(h,menu)!=menu:
                    raise ValueError(f"Move choice depends on arrival ply at {h}")
            self.max_moves = max(self.max_moves,int(counts.max(initial=0)))
            # Per-child maximum across mutually exclusive moves of each parent.
            e = l.edges.tocoo()
            if len(e.data):
                key = l.owners[e.row]*l.edges.shape[1]+e.col
                sort = np.argsort(key)
                key = key[sort]
                at = np.r_[0,np.flatnonzero(np.diff(key))+1]
                row,col = np.divmod(key[at],l.edges.shape[1])
                trans = csr_matrix((np.maximum.reduceat(e.data[sort],at),(row,col)),
                                   shape=(len(l.ids),l.edges.shape[1]))
                cap = np.minimum(1.,trans.T@cap)
            else:
                cap = np.zeros(l.edges.shape[1])

    def back(self,penalty,book=None):
        vals = np.array([])
        margins = []
        for l,gids,slots in reversed(list(zip(self.levels,self.gids,self.slots))):
            action = l.base + l.edges@vals - penalty[gids[l.owners]]
            margins.append(action-l.stop[l.owners])
            vals = l.stop.copy()
            if book is None:
                np.maximum.at(vals,l.owners,action)
            else:
                use = book[gids[l.owners]]==slots
                vals[l.owners[use]] = action[use]
        return float(self.start@vals),margins[::-1]

    def forward(self,book):
        reach = self.start.copy()
        arrivals = []
        paid_reach = np.zeros(len(self.hashes))
        income = (reach>0).astype(int)
        routes = np.zeros(len(self.hashes),dtype=int)
        value = 0.
        for l,gids,slots in zip(self.levels,self.gids,self.slots):
            arrivals.append(reach)
            idx = np.flatnonzero((book[gids[l.owners]]==slots)&(reach[l.owners]>0))
            owners = l.owners[idx]
            mass = reach[owners]
            value += float(reach@l.stop + mass@(l.base[idx]-l.stop[owners]))
            paid_reach[gids[owners]] += mass
            routes[gids[owners]] += income[owners]
            picked = l.edges[idx]
            reach = picked.T@mass
            income = picked.getnnz(axis=0)
        return value,arrivals,paid_reach,routes

    def collective_margins(self,margins,weights):
        total = np.zeros((len(self.hashes),max(1,self.max_moves)))
        for l,gids,slots,d,w in zip(self.levels,self.gids,self.slots,margins,weights):
            total[gids[l.owners],slots] += d*w[l.owners]
        return total

    def optimize(self,budget):
        shape = len(self.hashes)
        zero = np.zeros(shape)
        best = np.full(shape,-1,dtype=int)
        best_value = self.forward(best)[0]
        upper = self.back(zero)[0]
        reference = self.capacity.copy()
        near_over = None
        for rnd in range(3):
            lo,hi = -32.,0.
            for _ in range(32):
                mid = (lo+hi)*.5
                price = np.exp(mid)
                cost = np.divide(price,reference,out=np.full(shape,1e8),where=reference>0)
                relaxed,margins = self.back(cost)
                if rnd==0:
                    # Expected charges under this relaxation are at most lambda per paid hash.
                    upper = min(upper,relaxed+price*budget)
                votes = self.collective_margins(margins,self.occurrence_cap)
                b = votes.argmax(axis=1)
                b[votes.max(axis=1)<=0.] = -1
                score,_,paid,_ = self.forward(b)
                b[paid<=0.] = -1
                size = np.count_nonzero(b>=0)
                if size<=budget and score>best_value:
                    best,best_value = b,score
                if size>budget and (near_over is None or size<near_over[0]):
                    near_over = (size,b.copy())
                if size>budget:
                    lo = mid
                else:
                    hi = mid
            _,arrivals,_,_ = self.forward(best)
            visited = zero.copy()
            for gids,w in zip(self.gids,arrivals):
                visited[gids] += w
            reference = np.where(visited>0,visited,self.capacity)
        starts = [best]
        if near_over is not None:
            starts.append(near_over[1])
        for book in starts:
            b = book.copy()
            for _ in range(200):
                score,arrivals,paid,_ = self.forward(b)
                b[paid<=0] = -1
                size = int(np.sum(b>=0))
                if size<=budget and score>=best_value:
                    best,best_value = b.copy(),score
                if size==budget:
                    break
                _,margins = self.back(zero,b)
                gains = self.collective_margins(margins,arrivals)
                if size>budget:
                    used = np.flatnonzero(b>=0)
                    order = used[np.argsort(gains[used,b[used]],kind="stable")]
                    b[order[:min(16,size-budget)]] = -1
                else:
                    slots = gains.argmax(axis=1)
                    v = gains.max(axis=1)
                    eligible = np.flatnonzero((b<0)&(v>0))
                    if not len(eligible):
                        break
                    order = eligible[np.argsort(-v[eligible],kind="stable")]
                    add = order[:min(32,budget-size)]
                    b[add] = slots[add]
        # An alias may carry a large potential reach on a route the chosen book
        # never takes. Revisit move switches using actual arrivals and verify
        # their joint effect, including any newly unreachable cards.
        for _ in range(24):
            _,arrivals,_,_ = self.forward(best)
            _,margins = self.back(zero,best)
            gains = self.collective_margins(margins,arrivals)
            used = np.flatnonzero(best>=0)
            if not len(used):
                break
            changed = gains[used]-gains[used,best[used],None]
            for row,i in enumerate(used):
                changed[row,len(self.menus[self.hashes[i]]):] = -np.inf
            alternatives = changed.argmax(axis=1)
            order = np.argsort(-changed[np.arange(len(used)),alternatives],kind="stable")
            improved = False
            for row in order[:12]:
                if changed[row,alternatives[row]]<=1e-13:
                    break
                trial = best.copy()
                trial[used[row]] = alternatives[row]
                score,_,paid,_ = self.forward(trial)
                if score>best_value+1e-13:
                    trial[paid<=0] = -1
                    best,best_value = trial,score
                    improved = True
                    break
            if not improved:
                break
        # A switch can release other cards. Spend that slack on reachable gains.
        for _ in range(budget):
            size = np.count_nonzero(best>=0)
            if size>=budget:
                break
            _,arrivals,_,_ = self.forward(best)
            _,margins = self.back(zero,best)
            gains = self.collective_margins(margins,arrivals)
            eligible = np.flatnonzero((best<0)&(gains.max(axis=1)>1e-13))
            if not len(eligible):
                break
            order = eligible[np.argsort(-gains[eligible].max(axis=1),kind="stable")]
            trial = best.copy()
            add = order[:min(16,budget-size)]
            trial[add] = gains[add].argmax(axis=1)
            score = self.forward(trial)[0]
            if score<=best_value:
                break
            best,best_value = trial,score
        if best_value>upper+1e-8:
            raise ValueError("Feasible policy exceeds global price bound")
        return {h:self.menus[h][s] for h,s in zip(self.hashes,best) if s>=0},upper

    def replay(self,policy):
        book = np.full(len(self.hashes),-1,dtype=int)
        for h,m in policy.items():
            book[self.index[h]] = self.menus[h].index(m)
        value,arrivals,paid,routes = self.forward(book)
        back,_ = self.back(np.zeros(len(book)),book)
        if abs(value-back)>1e-9 or np.any((book>=0)&(paid<=0)):
            raise ValueError("Invalid replay or unreachable study card")
        used = [set(gids[(book[gids]>=0)&(w>0)].tolist()) for gids,w in zip(self.gids,arrivals)]
        return dict(value=value,cards=len(policy),cards_by_depth=[len(u) for u in used],
                    cross_ply_repeat_charges_avoided=sum(map(len,used))-len(policy),
                    multiply_reached_cards=int(np.sum(routes>1)),
                    in_book_mass_by_depth=[float(w[book[g]>=0].sum()) for w,g in zip(arrivals,self.gids)])

    def lp_bound(self,budget):
        """Relax one global purchase per SAN to [0,1]. Keep flow conservation,
        compulsory use of each paid board and a distinct global card budget."""
        offset = np.cumsum([0]+[len(l.moves) for l in self.levels])
        cards = {(h,m):j for j,(h,m) in enumerate((h,m) for h in self.hashes for m in self.menus[h])}
        card = {hm:int(offset[-1])+j for hm,j in cards.items()}
        nvars = offset[-1]+len(card)
        c = np.zeros(nvars)
        rows,cols,vals,rhs = [],[],[],[]
        def limit(terms,b):
            row = len(rhs)
            for col,val in terms:
                rows.append(row)
                cols.append(col)
                vals.append(val)
            rhs.append(b)
        limit(((j,1.) for j in card.values()),budget)
        for h,menu in self.menus.items():
            if len(menu)>1:
                limit(((card[h,m],1.) for m in menu),1.)
        for d,l in enumerate(self.levels):
            prev = self.levels[d-1].edges.tocsc() if d else None
            wnext = self.levels[d+1].stop if d+1<len(self.levels) else np.array([])
            c[offset[d]:offset[d+1]] = -1e5*(l.base+l.edges@wnext-l.stop[l.owners])
            starts = np.searchsorted(l.owners,np.arange(len(l.ids)+1))
            for i,h in enumerate(l.ids):
                if starts[i]==starts[i+1]:
                    continue
                cap = self.occurrence_cap[d][i]
                root = 0. if d else self.start[i]
                if d:
                    sl = slice(prev.indptr[i],prev.indptr[i+1])
                    incoming = [(offset[d-1]+j,p) for j,p in zip(prev.indices[sl],prev.data[sl])]
                else:
                    incoming = []
                fs,purchases = [],[]
                for a in range(starts[i],starts[i+1]):
                    f,y = offset[d]+a,card[h,l.moves[a]]
                    fs.append((f,1.))
                    purchases.append((y,cap))
                    limit([(f,1.),(y,-cap)],0.)
                limit(fs+[(j,-p) for j,p in incoming],root)
                limit(incoming+[(j,-v) for j,v in fs]+purchases,cap-root)
        mat = coo_matrix((vals,(rows,cols)),shape=(len(rhs),nvars)).tocsr()
        res = linprog(c,A_ub=mat,b_ub=rhs,bounds=(0,1),method="highs",options={"time_limit":90.})
        if not res.success:
            return None
        # A dual-feasible upper bound even in the presence of small numerical
        # residuals: minimize c.x + y.(A.x-b) over the explicit [0,1] box.
        y = np.maximum(0.,-res.ineqlin.marginals)
        lower = np.minimum(0.,c+mat.T@y).sum()-y@rhs
        return float(self.start@self.levels[0].stop-lower/1e5+1e-10)
