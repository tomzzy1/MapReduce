#pragma once
#include <vector>
#include <string>
#include <iostream>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/utility.hpp>

class Table
{
public:
	Table() = default;
	Table(const std::vector<std::vector<std::string>>& t
		, const std::vector<std::string>& a): v(t), attr(a) {}
	auto begin()
	{
		return v.begin();
	}
	auto end()
	{
		return v.end();
	}
	void update(Table t)
	{
	    v = t.v;
	}
	void push_back(std::vector<std::string>&& row)
	{
		v.push_back(std::move(row));
	}
	Table& operator+(Table& t)
	{
		v.insert(v.end(), t.begin(), t.end());
		return *this;
	}
	auto attribute() const
	{
		return attr;
	}
	std::vector<std::vector<std::string>> split(size_t size, size_t rank) const
	{
		auto begin = v.begin() + v.size() / size * rank;
		auto end = v.begin() + v.size() / size * (rank + 1);
		return std::vector<std::vector<std::string>>(begin, size - 1 == rank ? v.end() : end);
	}
	template<typename Archive>
	void serialize(Archive& ar, const unsigned int version)
	{
		ar & v;
		ar & attr;
	}
	void print()
	{
		for (auto& row : v)
		{
			for (auto& str : row)
			{
				std::cout << str << ' ';
			}
			std::cout << '\n';
		}
		std::cout << std::endl;
	}

	std::vector<std::vector<std::string>> v;
	std::vector<std::string> attr;
};

