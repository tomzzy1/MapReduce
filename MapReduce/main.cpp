#include <boost/mpi/environment.hpp>
#include <boost/mpi/communicator.hpp>
#include <iostream>
#include "Table.hpp"
#include "MapReduce.hpp"
#include "Selector.hpp"
#include "Projector.hpp"
#include "Joiner.hpp"
namespace mpi = boost::mpi;

int main()
{
    mpi::environment env;
    mpi::communicator world;
    
    Table t;
    Table t2;
    Table t3;
    auto pred = [](auto v) { return v[0] == "a"; };
    Selector<decltype(pred)> sel(pred);
    MapReduce<Selector<decltype(pred)>> mr(world, sel);
    //Projector proj({ 0, 1, 3 });

    
    if (world.rank() == 0)
    {
        t = std::vector<std::vector<std::string>>
        {
            std::vector<std::string>{"a","1","2","3"},
            std::vector<std::string>{"a","1","2","3"},
            std::vector<std::string>{"b","1","2","3"},
            std::vector<std::string>{"c","1","2","3"},
            std::vector<std::string>{"b","1","2","3"},
            std::vector<std::string>{"a","1","2","3"},
            std::vector<std::string>{"a","1","2","3"},
            std::vector<std::string>{"d","1","2","3"},
            std::vector<std::string>{"b","1","2","3"},
        };
        t2 = std::vector<std::vector<std::string>>
        {
            std::vector<std::string>{"a","1"},
            std::vector<std::string>{"b","1"},
            std::vector<std::string>{"b","2"},
            std::vector<std::string>{"c","2"},
        };
        t3 = std::vector<std::vector<std::string>>
        {
            std::vector<std::string>{"1","A"},
            std::vector<std::string>{"1","B"},
            std::vector<std::string>{"2","C"},
            std::vector<std::string>{"3","C"},
        };

    }
    mr.start(t);

    MapReduce<Projector> mr2(world, { 0, 1, 3 });
    mr2.start(t);

    MapReduce<Joiner> mr3(world, Joiner({ 1 }, { 0 }));
    mr3.start({ t2,t3 });
    return 0;
}



