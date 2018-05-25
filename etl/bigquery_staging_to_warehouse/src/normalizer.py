######### Sale Order ###########
def sale_order_line_extra_normalizer(extras):
    result = []
    if len(extras) > 0:
        for extra in extras:
            
            # Process price for extra toppings
            extra_price = None
            if 'price' in extra:
                if extra['price'] != None:
                    extra_price = float(extra['price']) * 1.0
                else:
                    extra_price = None
            else:
                extra_price = None
            
            # Process quantity for extra toppings
            extra_quantity = None
            if 'quantity' in extra:
                if extra['quantity'] != None:
                    extra_quantity = float(extra['quantity']) * 1.0
                else:
                    extra_quantity = None
            else:
                extra_quantity = None
                
            # Construct extra object
            extra_normalized = {
                'price': extra_price,
                'quantity': extra_quantity,
                'product_id': extra['product_id'] if 'product_id' in extra else None,
                'val': extra['val'] if 'val' in extra else None,
            }
            result.append(extra_normalized)
    else:
        result = None
    
    return result


# In[62]:


def sale_order_line_normalizer(orderlines):
    result = []
    if len(orderlines) > 0:
        for item in orderlines:
            item_normalized = {
                'product_id': item['product_id'],
                'price': float(item['price']) * 1.0,
                'quantity': float(item['quantity']) * 1.0,
                'val': item['val'] if 'val' in item else None,
                'extra': sale_order_line_extra_normalizer(item['extra']) if 'extra' in item else None,
            }
            result.append(item_normalized)
    else:
        result = None
    
    return result


# In[63]:


def sale_order_nomalizer(order):
    return {
            'order_id': str(order['_id']),
            'ref': str(order['ref']),
            'src': order['src'],
            'total': float(order['total']) * 1.0,
            'customer_id': order['customer']['customer_id'] if 'customer' in order else None,
            'discount': float(order['discount']) * 1.0,
            'check_auto': order['check_auto'],
            'subtotal': float(order['subtotal']) * 1.0,
            'store_id': order['shop']['id'] if all(['shop' in order, type(order['shop']) is dict]) else None,
            'store_pos_id': order['shop']['pos_id'] if all(['shop' in order, type(order['shop']) is dict]) else None,
            'note': order['note'],
            'coupon_code': order['coupon_code'],
            'status': order['status'],
            'created_at': order['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': order['updated_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'sale_person_id': order['sale_person']['id'] if 'sale_person' in order else None,
            'shipper_distance': order['shipper']['distance'] if 'shipper' in order else None,
            'shipper_delivery_type': order['shipper']['delivery_type'] if 'shipper' in order else None,
            'order_line': sale_order_line_normalizer(order['orderlines']) if 'orderlines' in order else None,
            'delivery_address_street': order['deliveryAddress']['address']['street'] if 'deliveryAddress' in order else None,
            'delivery_address_full_address': order['deliveryAddress']['address']['full_address'] if 'deliveryAddress' in order else None,
            'delivery_address_lat': order['deliveryAddress']['address']['lat'] if 'deliveryAddress' in order else None,
            'delivery_address_lng': order['deliveryAddress']['address']['lng'] if 'deliveryAddress' in order else None,
        }

######### End Sale Order Normalizer #########