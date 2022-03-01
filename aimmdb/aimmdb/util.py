from collections import OrderedDict

import h5py

# def mk_path(c, path):
#    for i, p in enumerate(path):
#        path_str = "/" + "/".join(path[0 : i + 1])
#        doc = c.find_one({"path": path_str})
#        if doc is None:
#            doc = Node(name=p, path=path_str, structure_family="node", metadata={}, data=None)
#            result = c.insert_one(doc.dict())
#    return doc


# read an hdf5 group recursively into memory
def read_group(g, jsoncompat=False):
    out = {}
    for k, v in g.items():
        if isinstance(v, h5py.Group):  # group
            x = read_group(v, jsoncompat=jsoncompat)
        else:  # dataset
            if v.shape == ():  # scalar
                x = v[()]
                if type(x) is bytes:
                    x = x.decode("utf-8")
                else:
                    if jsoncompat:
                        x = x.item()
            else:  # array
                x = v[:]
                if jsoncompat:
                    x = x.tolist()
        out[k] = x
    return out


_symbols = [
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Po",
    "At",
    "Rn",
    "Fr",
    "Ra",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
    "Am",
    "Cm",
    "Bk",
    "Cf",
    "Es",
    "Fm",
    "Md",
    "No",
    "Lr",
    "Rf",
    "Db",
    "Sg",
    "Bh",
    "Hs",
    "Mt",
    "Ds",
    "Rg",
    "Cn",
    "Uut",
    "Fl",
    "Uup",
    "Lv",
    "Uus",
    "Uuo",
]

_edges = [
    "K",
    "L",
    "L1",
    "L2",
    "L3",
    "M",
    "M1",
    "M2",
    "M3",
    "M4",
    "M5",
    "N",
    "N1",
    "N2",
    "N3",
    "N4",
    "N5",
    "N6",
    "N7",
    "O",
    "O1",
    "O2",
    "O3",
    "O4",
    "O5",
    "O6",
    "O7",
]
