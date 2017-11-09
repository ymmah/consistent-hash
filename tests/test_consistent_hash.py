from __future__ import print_function

import unittest

import random
import string
import sys

from consistent_hash.consistent_hash import ConsistentHash

if sys.version_info[0] == 3:
    chars = string.ascii_letters + string.digits
else:
    chars = string.letters + string.digits


ConsistentHash.interleave_count = 1000


class TestConsistentHash(unittest.TestCase):
    init_nodes = {
        '192.168.0.101:11212': 1,
        '192.168.0.102:11212': 1,
        '192.168.0.103:11212': 1,
        '192.168.0.104:11212': 1,
    }
    obj_nums = 10000

    @classmethod
    def setup_class(cls):
        cls.objects = cls.generate_random_objects()
        print('Initial nodes {nodes}'.format(nodes=cls.init_nodes))

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        self.hit_nums = {}

    def tearDown(self):
        pass

    def test___init__(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # Get nodes from hashing ring
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (23, 27),
            '192.168.0.102:11212': (23, 27),
            '192.168.0.103:11212': (23, 27),
            '192.168.0.104:11212': (23, 27)
        })

    def test_empty__init__(self):
        self.con_hash = ConsistentHash()
        for obj in self.objects:
            node = self.con_hash.get_node(obj)

            if node is not None:
                raise AssertionError('Should have received an exception when hashing using an empty LUT')

        self.con_hash.add_nodes(self.init_nodes)

        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1

        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (23, 27),
            '192.168.0.102:11212': (23, 27),
            '192.168.0.103:11212': (23, 27),
            '192.168.0.104:11212': (23, 27),
        })

    def test_add_nodes(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # Add nodes to hashing ring
        add_nodes = {'192.168.0.105:11212': 1}
        self.con_hash.add_nodes(add_nodes)
        # Get nodes from hashing ring
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.105:11212': (17, 23),
            '192.168.0.102:11212': (17, 23),
            '192.168.0.104:11212': (17, 23),
            '192.168.0.101:11212': (17, 23),
            '192.168.0.103:11212': (17, 23),
        })
        print('->The {nodes} added!!!'.format(nodes=add_nodes))

    def test_add_nodes_unicode(self):
        self.con_hash = ConsistentHash({
            u'192.168.0.101:11212': 1,
            u'192.168.0.102:11212': 1,
            u'192.168.0.103:11212': 1,
            u'192.168.0.104:11212': 1,
        })
        # Add nodes to hashing ring
        add_nodes = u'192.168.0.105:11212'
        self.con_hash.add_nodes(add_nodes)
        # Get nodes from hashing ring
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.105:11212': (17, 23),
            '192.168.0.102:11212': (17, 23),
            '192.168.0.104:11212': (17, 23),
            '192.168.0.101:11212': (17, 23),
            '192.168.0.103:11212': (17, 23),
        })
        print('->The {nodes} added!!!'.format(nodes=add_nodes))

    def test_add_nodes_tuple(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # Add nodes to hashing ring
        add_nodes = ('192.168.0.105:11212', '192.168.0.106:11212')
        self.con_hash.add_nodes(add_nodes)
        # Get nodes from hashing ring
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.106:11212': (15, 17),
            '192.168.0.105:11212': (15, 17),
            '192.168.0.102:11212': (15, 17),
            '192.168.0.104:11212': (15, 17),
            '192.168.0.101:11212': (15, 17),
            '192.168.0.103:11212': (15, 17),
        })
        print('->The {nodes} added!!!'.format(nodes=add_nodes))

    def test_del_nodes(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # del_nodes = self.nodes[0:2]
        del_nodes = ['192.168.0.102:11212', '192.168.0.104:11212']
        # Delete the nodes from hashing ring
        self.con_hash.del_nodes(del_nodes)
        # Get nodes from hashing ring after deleting
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (48, 52),
            '192.168.0.103:11212': (48, 52)
        })
        print('->The {nodes} deleted!!!'.format(nodes=del_nodes))

    def test_del_nodes_tuple(self):
        self.con_hash = ConsistentHash(self.init_nodes)
        # del_nodes = self.nodes[0:2]
        del_nodes = ('192.168.0.102:11212', '192.168.0.104:11212')
        # Delete the nodes from hashing ring
        self.con_hash.del_nodes(del_nodes)
        # Get nodes from hashing ring after deleting
        for obj in self.objects:
            node = self.con_hash.get_node(obj)
            self.hit_nums[node] = self.hit_nums.get(node, 0) + 1
        distribution = self.show_nodes_balance()

        self.validate_distribution(distribution, {
            '192.168.0.101:11212': (48, 52),
            '192.168.0.103:11212': (48, 52)
        })
        print('->The {nodes} deleted!!!'.format(nodes=del_nodes))

    # -------------Help functions-------------
    def show_nodes_balance(self):
        distribution = {}
        print('-' * 67)
        print('Nodes count:{nNodes} Objects count:{nObjects}'.format(
            nNodes=self.con_hash.get_nodes_cnt(),
            nObjects=len(self.objects)
        ))
        print('-' * 27 + 'Nodes balance' + '-' * 27)

        for node in self.con_hash.get_all_nodes():
            substitutions = {
                'nNodes': node,
                'nObjects': self.hit_nums[node],
                'percentage': self.get_percent(self.hit_nums[node], self.obj_nums)
            }

            print('Nodes: {nNodes} \
                   - Objects count: {nObjects} \
                   - percent:{percentage}%'.format(**substitutions))

            distribution[node] = substitutions['percentage']

        return distribution

    @staticmethod
    def validate_distribution(actual, expected):
        if expected.keys() != actual.keys():
            raise AssertionError('Expected nodes does not match actual nodes')

        for i in expected.keys():
            actual_value = actual[i]
            min_value = expected[i][0]
            max_value = expected[i][1]

            if actual_value < min_value or actual_value > max_value:
                print(min_value, actual_value, max_value)
                raise AssertionError(
                    'Value {actual} outside of expected range ({expected1},{expected2})'.format(
                        expected1=min_value,
                        expected2=max_value,
                        actual=actual_value,
                    )
                )

        print('Validated ranges')

    @staticmethod
    def get_percent(numerator, denominator):
        return int(float(numerator) / denominator * 100)

    @staticmethod
    def generate_random_objects(num=10000, length=10):
        objects = []
        for i in range(num):
            objects.append(''.join([random.choice(chars) for _ in range(length)]))
        return objects

    def test_sample_hash_output(self):
        ConsistentHash.interleave_count = 40
        # Test backward compatibility with version 1.0
        samples = {
            '35132097': 'B',
            '25291004': 'D',
            '48182416': 'F',
            '45818378': 'H',
            '52733021': 'A',
            '94027025': 'I',
            '18116713': 'F',
            '75531098': 'J',
            '99011825': 'F',
            '99371754': 'A',
            '19630740': 'D',
            '87823770': 'G',
            '32160063': 'A',
            '28054420': 'E',
            '75904283': 'H',
            '08458048': 'E',
            '51583844': 'I',
            '16226754': 'B',
            '95450503': 'E',
            '47557476': 'C',
            '38808589': 'A',
        }

        hash_ring = ConsistentHash(objects=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'])
        for node, output in samples.items():
            result = hash_ring.get_node(node)
            if result != output:
                raise AssertionError('Expected node does not match actual node. Expected: {}. Got: {}'.format(
                        output,
                        result,
                    )
                )
