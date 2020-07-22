#pragma once
#include "Context.hpp"

class Joiner
{
public:
	Joiner(std::initializer_list<int> l1, std::initializer_list<int> l2) : first(l1), second(l2) {}
	Joiner(const Joiner&) = default;

	using InputType = std::pair<Table, Table>;
	using SplitType = std::vector<std::pair<int, std::vector<std::string>>>;
	using MapKeyType = std::vector<std::string>;
	using MapValueType = std::pair<int, std::vector<std::string>>;
	using ReduceKeyType = MapKeyType;
	using ReduceValueType = Table;

	SplitType split(const InputType& input, int size, int rank)
	{
		SplitType res;
		auto first_split = input.first.split(size, rank);
		auto second_split = input.second.split(size, rank);
		for (auto& i : first_split)
		{
			res.emplace_back(0, i);
		}
		for (auto& i : second_split)
		{
			res.emplace_back(1, i);
		}
		return res;
	}

	void map(SplitType value, Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		for (auto& row : value)
		{
			if (row.first == 0)
			{
				MapKeyType new_key;
				std::vector<std::string> new_value;
				for (int i = 0; i < row.second.size(); ++i)
				{
					if (std::find(first.begin(), first.end(), i) != first.end())
					{
						new_key.push_back(row.second[i]);
					}
					else
					{
						new_value.push_back(row.second[i]);
					}
				}
				c.write(new_key, std::pair<int, std::vector<std::string>>(0, new_value));
			}
			if (row.first == 1)
			{
				MapKeyType new_key;
				std::vector<std::string> new_value;
				for (int i = 0; i < row.second.size(); ++i)
				{
					if (std::find(second.begin(), second.end(), i) != second.end())
					{
						new_key.push_back(row.second[i]);
					}
					else
					{
						new_value.push_back(row.second[i]);
					}
				}
				c.write(new_key, std::pair<int, std::vector<std::string>>(1, new_value));
			}
		}
	}

	void reduce(MapKeyType key, std::vector<MapValueType> value,
		Context<MapKeyType, MapValueType, ReduceKeyType, ReduceValueType>& c)
	{
		std::vector<MapKeyType> first_table;
		std::vector<MapKeyType> second_table;
		for (auto& val : value)
		{
			if (val.first == 0)
				first_table.push_back(val.second);
			else
				second_table.push_back(val.second);
		}
		if (first_table.empty() || second_table.empty())
			return;
		ReduceValueType table;
		for (auto& i : first_table)
		{
			for (auto& j : second_table)
			{
				auto new_row = i;
				new_row.insert(new_row.end(), key.begin(), key.end());
				new_row.insert(new_row.end(), j.begin(), j.end());
				table.push_back(std::move(new_row));
			}
		}
		c.write(key, table);
	}

private:
	std::vector<int> first;
	std::vector<int> second;
};