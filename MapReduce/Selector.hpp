#pragma once
#include "Context.hpp"

template<typename Predicate>
class Selector
{
public:
	using InputType = Table;
	using SplitType = std::vector<std::vector<std::string>>;
	using MapKeyType = int;
	using MapValueType = std::vector<std::string>;
	using ReduceKeyType = int;
	using ReduceValueType = Table;

	Selector(Predicate p) :pred(p) {}

	SplitType split(const InputType& input, int size, int rank)
	{
		return input.split(size, rank);
	}

	void map(SplitType value, Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		for (auto& row : value)
		{
			if (pred(row))
			{
				c.write(1, row);
			}
			else
			{
				c.write(0, row);
			}
		}
	}

	void reduce(MapKeyType key, std::vector<MapValueType> value, 
		Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		if (key == 1)
		{
			Table t;
			for (auto&& row : value)
			{
				t.push_back(std::move(row));
			}
			c.write(key, t);
		}
	}

private:
	Predicate pred;
};

