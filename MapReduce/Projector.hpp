#pragma once
#include "Context.hpp"

class Projector
{
public:
	Projector(std::initializer_list<int> l) : v(l) {}
	Projector(const Projector&) = default;

	using InputType = Table;
	using SplitType = std::vector<std::vector<std::string>>;
	using MapKeyType = std::vector<std::string>;
	using MapValueType = std::vector<std::string>;
	using ReduceKeyType = Table;
	using ReduceValueType = Table;

	SplitType split(const InputType& input, int size, int rank)
	{
		return input.split(size, rank);
	}

	void map(SplitType value, Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		for (auto& row : value)
		{
			MapValueType new_row;
			for (int i : v)
			{
				new_row.push_back(row[i]);
			}
			c.write(new_row, new_row);
		}
	}

	void reduce(MapKeyType key, std::vector<MapValueType> value,
		Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		Table t;
		t.push_back(std::move(value.front()));
		c.write(t, t);
	}

private:
	std::vector<int> v;
};

