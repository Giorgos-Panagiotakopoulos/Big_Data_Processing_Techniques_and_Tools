def neighborCells(H, W, bin):
    n1 = -1
    n2 = -1
    n5 = -1
    n3 = -1
    n4 = -1
    n6 = -1
    n7 = -1
    n8 = -1

    n1 = bin + H
    # bin not in upper row
    if bin % H < H - 1:
        n2 = bin + H + 1
    # bin not in lower row
    if bin % H != 0:
        n3 = bin + H - 1
    # bin not in upper row
    n4 = bin - H

    # bin not in upper row
    if bin % H < H - 1:
        n5 = bin - H + 1

    # bin not in lower row
    if bin % H != 0:
        n6 = bin - H - 1

    # bin not in upper row
    if bin % H < H - 1:
        n7 = bin + 1

    if bin % H != 0:
        n8 = bin - 1

    neighbors = []
    if n1 >= 0 and n1 <= W * H - 1:
        neighbors.append(n1)
    if n2 >= 0 and n2 <= W * H - 1:
        neighbors.append(n2)
    if n3 >= 0 and n3 <= W * H - 1:
        neighbors.append(n3)
    if n4 >= 0 and n4 <= W * H - 1:
        neighbors.append(n4)
    if n5 >= 0 and n5 <= W * H - 1:
        neighbors.append(n5)
    if n6 >= 0 and n6 <= W * H - 1:
        neighbors.append(n6)
    if n7 >= 0 and n7 <= W * H - 1:
        neighbors.append(n7)
    if n8 >= 0 and n8 <= W * H - 1:
        neighbors.append(n8)
    neighbors.sort()
    return neighbors

def neighborCellsStr(H, W, bin):
    n1 = -1
    n2 = -1
    n5 = -1
    n3 = -1
    n4 = -1
    n6 = -1
    n7 = -1
    n8 = -1

    n1 = bin + H
    # bin not in upper row
    if bin % H < H - 1:
        n2 = bin + H + 1
    # bin not in lower row
    if bin % H != 0:
        n3 = bin + H - 1
    # bin not in upper row
    n4 = bin - H

    # bin not in upper row
    if bin % H < H - 1:
        n5 = bin - H + 1

    # bin not in lower row
    if bin % H != 0:
        n6 = bin - H - 1

    # bin not in upper row
    if bin % H < H - 1:
        n7 = bin + 1

    if bin % H != 0:
        n8 = bin - 1

    neighbors = []
    s = ""
    if n1 >= 0 and n1 <= W * H - 1:
        neighbors.append(n1)
    if n2 >= 0 and n2 <= W * H - 1:
        neighbors.append(n2)
    if n3 >= 0 and n3 <= W * H - 1:
        neighbors.append(n3)
    if n4 >= 0 and n4 <= W * H - 1:
        neighbors.append(n4)
    if n5 >= 0 and n5 <= W * H - 1:
        neighbors.append(n5)
    if n6 >= 0 and n6 <= W * H - 1:
        neighbors.append(n6)
    if n7 >= 0 and n7 <= W * H - 1:
        neighbors.append(n7)
    if n8 >= 0 and n8 <= W * H - 1:
        neighbors.append(n8)
    neighbors.sort()
    for item in neighbors:
        s = s + str(item) + ", "
    s = s.strip(", ")
    return s