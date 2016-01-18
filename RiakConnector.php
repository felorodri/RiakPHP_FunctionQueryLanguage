<?php

/*
 * File:        RiakConnector.php 
 * Class:       RiakConnector
 * Created by:  Julián Rodríguez
 * Description: Provide an object object with the connection stablished to one node of a Riak cluster.
 *              Provide multiple functions to perform the basic CRUD operations.
 *              This connector is under active development, there are still much to do, some things like validations,
 *              check if exist before perform any operation and many others things.
 * Created on:  Febrary 17 2015
*/

   
    require_once('riak-php-client/src/Basho/Riak/Riak.php');
    require_once('riak-php-client/src/Basho/Riak/Bucket.php');
    require_once('riak-php-client/src/Basho/Riak/Exception.php');
    require_once('riak-php-client/src/Basho/Riak/Link.php');
    require_once('riak-php-client/src/Basho/Riak/MapReduce.php');
    require_once('riak-php-client/src/Basho/Riak/Object.php');
    require_once('riak-php-client/src/Basho/Riak/StringIO.php');
    require_once('riak-php-client/src/Basho/Riak/Utils.php');
    require_once('riak-php-client/src/Basho/Riak/Link/Phase.php');
    require_once('riak-php-client/src/Basho/Riak/MapReduce/Phase.php');



Class RiakConnector {

    private $client = null;

    /* Function: __construct
     * Description: Constructor method.
     *              $ip must be strings.
     *              $port must be an int.
     */
    
    public function __construct($ip, $port) {
        
                # Setting the Riak connection
                $this->client = new Basho\Riak\Riak($ip, $port);
                
    }


    /* Function: insert
     * Description: Insert a document into a specific bucket.
     *              $data must be an array ie: array('name' => "John Smith",'age' => 28,'company' => "Facebook")
     *              $bucket must be a string.
     *              $id could be null or a string. If it is null the automated id is assigned by Riak.
     *              $primary_indexes may be null or an array wich contains an array in each position following this form: array(array('field_name','index_type').
     *                  The field type must be an string between 'int' and 'bin'
     *              $secondary_indexes may be null or an array wich contains an array in each position following this form: array(array('index_name', 'index_type', 'index_value'))
     *                index_type must be one of these strings: 'bin', 'int'.
     *              $links may be null or an array wich contains an array in each position following this form: array(array('link_name','bucket','linked_object_key'))
     *
     */
    
    public function insert($data, $bucket, $id = null, $primary_indexes = null, $secondary_indexes = null, $links = null){

        # Select a bucket
        $selected_bucket = $this->client->bucket($bucket);
        $object = null;

        
        if(is_null($id)){

            $object = $selected_bucket->newObject('',$data);
                    
        } else{

                $object = $selected_bucket->newObject($id, $data);

            }

        if(!is_null($primary_indexes)){
            
            $object = $this->addIndex($primary_indexes, $object, true);

        }

        if(!is_null($secondary_indexes)){
            
            $object = $this->addSecondaryIndex($secondary_indexes, $object, true);
            
        }

        if(!is_null($links)){
            
            $object = $this->add_Link($links, $object, true);
            
        }

        
        # Connectign Riak to save the object
        try{

            $object->store();
            
            
        }catch(Exception $e){

            echo "Riak server not available";            

        }

        
        
            
    }


    /* Function: findOne
     * Description: Find a specific document by a key and return the Riak object data as a json if $return_object is false, otherwise return the Riak Object.
     *              $key and $bucket, both must be strings. 
     *              $return_object must be a boolean. If true the object is returned, if false the data of the object is returned.
     */
    
    public function findOne($key, $bucket, $return_object=false){
        
        try{    
                
                # Setting bucket
                $selected_bucket = $this->client->bucket($bucket);
                # Connecting to Riak
                $object = $selected_bucket->get($key);
                
                if(!$return_object){

                    $data = $object->getData();
                    return json_encode($data);
                    //return $data;

                }else{

                    return $object;

                }
            }catch(Exception $e){

                echo "Riak server not available";

            }
    }


    /* Function: findOne
     * Description: Find all objects in a given buckets and return them or its data.
     *              $bucket must be strings. 
     *              $return_object must be a boolean. If true the object is returned, if false the data of the object is returned.
     */
    
    public function findAll($bucket, $return_object=false){
        
        try{    
                //print_r('Entered findAll');
                $response = array();

                # Setting bucket
                $selected_bucket = $this->client->bucket($bucket);
                # Connecting to Riak
                $keys = $selected_bucket->getKeys();
               
                if(!$return_object){
                    //print_r('Entered if...');
                    foreach ($keys as $key => $value) {
                        
                        $object = $this->findOne($value, $bucket);
                        $response[] = json_encode($object);

                    }

                }else{

                    foreach ($keys as $key => $value) {
                        
                        $object = $this->findOne($value, $bucket, true);
                        $response[] = $object;

                    }

                }

                return $response;
                
            }catch(Exception $e){

                echo "Riak server not available";

            }
    }





    /* Function: findByIndex
     * Description: Returns an array. 
                    $index_name must be a string
                    $index_type must be one of this strings: "bin" or "int"
                    $index_value may be a string or an int
                    $bucket must be a string
                    $return_objects must be a boolean. If true the response array will contain each matched object, 
                        if false, the data of each matched object will be returned y the array.
     */
                    

    /* IMPORTANT NOTE: THIS FUNCTION REQUIRE THE storage_backend SET TO leveldb IN THE Riak.conf file of the node */
    /*                 This function may cause problems if the half of your ring or more nodes are down.          */

    public function findByIndex($index_name, $index_type, $index_value, $bucket, $return_objects = false){
        
        # Setting bucket

        $selected_bucket = $this->client->bucket($bucket);
        $results = $selected_bucket->indexSearch($index_name, $index_type, $index_value);
        $response = array();

        if(!$return_objects){
            
            foreach ($results as $link) {
                //echo "Key: {$link->getKey()}<br/>";
                $object = $link->get();
                $response[] = json_encode($object->data);
            }

        }else{

            foreach ($results as $link) {
                //echo "Key: {$link->getKey()}<br/>";
                $object = $link->get();
                $response[] = $object;
            }

        }
        
        return $response;
        
    }


    /* Function: findByIndexRange
     * Description: Retrieve an array. Only available for index_type int
                    $index_name must be a string
                    $index_value may be a string or an int
                    $bucket must be a string
                    $return_objects must be a boolean. If true the response array will contain each matched object, 
                        if false, the data of each matched object will be returned y the array.
     */
                    

    /* IMPORTANT NOTE: THIS FUNCTION REQUIRE THE storage_backend SET TO leveldb IN THE Riak.conf file of the node */
    /*                 This function may cause problems if the half of your ring or more nodes are down.          */
    
    public function findByIndexRange($index_name, $index_value1, $index_value2, $bucket, $return_objects = false, $duplicates = false){
        
        # Setting bucket

        $selected_bucket = $this->client->bucket($bucket);
        $results = null;

        if(!$duplicates){

            $results = $selected_bucket->indexSearch($index_name, "int", $index_value1, $index_value2, true);

        }else{

            $results = $selected_bucket->indexSearch($index_name, "int", $index_value1, $index_value2);            

        }
        
        $response = array();

        if(!$return_objects){
            
            foreach ($results as $link) {
                //echo "Key: {$link->getKey()}<br/>";
                $object = $link->get();
                $response[] = $object->data;
            }

        }else{

            foreach ($results as $link) {
                //echo "Key: {$link->getKey()}<br/>";
                $object = $link->get();
                $response[] = $object;
            }

        }
        
        return $response;
        
    }

    
    /* Function: update
     * Description: Retrieve a specific document by a key, modify some specific fields and save it back to Riak.
     *              $key and $bucket, both must be strings.
     *              $update_vars must be an array ie: array('company' => "google",'age' => 56)
     */
    
    public function update($key, $bucket, $update_vars){
        
        $object = $this->findOne($key, $bucket, true);
        $data = $object->getData();
        
        foreach ($update_vars as $key => $value) {
            
            if(array_key_exists($key, $data)){

                # Update the object
                $object->data[$key] = $value;    

            }
            
        }
        
        # Save it back to Riak
        $object->store();

    }


    /* Function: addNewInfo
     * Description: Retrieve a specific document by a key and append a non existing info given.
     *              $key and $bucket, both must be strings.
     *              $data must be an array ie: array('name' => "John Smith",'age' => 28,'company' => "Facebook")
     */

    public function addNewInfo($key, $bucket, $data){
        
        $object = $this->findOne($key, $bucket, true);
        $stored_data = $object->getData();
        $new_data = array_merge($stored_data,$data);
        $object->setData($new_data);
        # Save it back to Riak
        $object->store();
       
    }


    /* Function: removeInfo
     * Description: Retrieve a specific document by a key and delete some specific field if exist.
     *              $key and $bucket, both must be strings.
     *              $data must be an array in a format like: array('field1', 'field2', ... , 'fieldn')
     */

    
    public function removeInfo($key, $bucket, $data){
        
        $object = $this->findOne($key, $bucket, true);
        $stored_data = $object->getData();
        
        foreach ($data as $key) {

            unset($stored_data[$key]);

        }
        
        $object->setData($stored_data);
        # Save it back to Riak
        $object->store();
       
    }


    /* Function: remove
     * Description: Retrieve a specific document by a key, then delete it from Riak.
     *              $key and $bucket, both must be strings.
     */
    
    public function remove($key, $bucket){
        
        
        
        $object = $this->findOne($key, $bucket, true);
        
        # Delete it from Riak

        try{

            $object->delete();

        }catch(Exception $e){

                echo "Riak server not available";
        }
        
       
    }


    /* Function: add_Link
     * Description: Named with underlined space because of the existence of an addLink function in Riak library.
     *              Retrieve a specific document by a key, add some links to it then save it back on Riak.
     *              $links must be an array wich contains an array in each position following this form: array(array('link_name','bucket','linked_object_key')).
     *              $object is the riak object in wich you want to add one or more links.
     *              $return_object is a false boolean by default. It determinate if return the object or save it back to Riak.
     */

    public function add_Link($links, $object, $return_object = false){

        $modified_object = $object;

        foreach ($links as $value) {
                
                // $value is like array(array('link_name','bucket','key'))
                $linked_obj = $this->findOne($value[2], $value[1], true);
                $modified_object->addLink($linked_obj, $value[0]);                

            }


        if(!$return_object){

            try{

                $modified_object->store();

            }catch(Exception $e){

                    echo "Riak server not available";            

                }    

        }else{

            return $modified_object;

        }

        
        
    }


    /* Function: remove_Link
     * Description: Named with underlined space because of the existence of an removeLink function in Riak library.
     *              Retrieve a specific document by a key, remove some links from it then save it back on Riak.
     *              $links must be an array wich contains an array in each position following this form: array(array('link_name','bucket','linked_object_key')).
     *              $object is the riak object in wich you want to remove one or more links.
     *              $return_object is a false boolean by default. It determinate if return the object or save it back to Riak.
     */

    private function remove_Link($links, $object, $return_object = false){


        $modified_object = $object;

        foreach ($links as $value) {
                
                // $value is like array(array('link_name','bucket','key'))
                $linked_obj = $this->findOne($value[2], $value[1], true);
                $modified_object->removeLink($linked_obj, $value[0]);

            }


        if(!$return_object){

            try{

                $modified_object->store();

            }catch(Exception $e){

                    echo "Riak server not available";            

                }    

        }else{

            return $modified_object;

        }
       
    }


    /* Function: addIndex 
     * Description: Known as autoIndexes. Receive an object, add some primary indexes to it then save it back on Riak. 
     *              $primary_indexes must be an array wich contains an array in each position following this form: array(array('field_name','field_type).
     *              $object is the riak object in wich you want to add one or more primary indexes.
     *              $return_object is a false boolean by default. It determinate if return the object or save it back to Riak.
     *              Auto indexes are kept fresh with the associated field automatically, so if you read an object, modify its data, and write it back, the auto index will reflect the new value from the object.
     */

    public function addIndex($primary_indexes, $object, $return_object = false){

        $modified_object = $object;
        
        foreach ($primary_indexes as $value) {
                
                $modified_object->addAutoIndex($value[0], $value[1]);
                
        }

        if(!$return_object){

            try{

                $modified_object->store();

            }catch(Exception $e){

                    echo "Riak server not available";            

                }    

        }else{

            return $modified_object;

        }        

        
    }
    


    /* Function: addSecondaryIndex
     * Description: Receive an object, add some secondary indexes to it then save it back on Riak.
     *              $secondary_indexes must be an array wich contains an array in each position following this form: array(array('index_name', 'index_type', 'index_value'))
     *              $object is the riak object in wich you want to add one or more secondary indexes
     *              $return_object is a false boolean by default. It determinate if return the object or save it back to Riak.
     */

    private function addSecondaryIndex($secondary_indexes, $object, $return_object = false){

        $modified_object = $object;
        
        foreach ($secondary_indexes as $value) {
                
                $modified_object->addIndex($value[0], $value[1], $value[2]);
                
            }

        if(!$return_object){

            try{

                $modified_object->store();

            }catch(Exception $e){

                    echo "Riak server not available";            

                }    

        }else{

            return $modified_object;

        }        
        
    }
   

    /* Function: removeIndex
     * Description: Remove autoIndexes. Receive an object, remove some (or all) primary indexes from it then save it back on Riak. 
     *              $primary_indexes may be an emptyarray or an array wich contains an array in each position following this form: array(array('field_name','field_type, remove_all) to remove an specific index. 
     *                  remove_all must be a true or false boolean
     *              $object is the riak object in wich you want to add one or more primary indexes.
     *              Auto indexes are kept fresh with the associated field automatically, so if you read an object, modify its data, and write it back, the auto index will reflect the new value from the object.
     */

    
    public function removeIndex($primary_indexes, $object){

        $modified_object = $object;
                
        foreach ($primary_indexes as $key) {
            
            $key_length = count($key);
                        
            if($key_length === 0){

                $modified_object->removeAllAutoIndexes();

            }else if ($key_length === 3) {
                
                if($key[2]===true){

                    $modified_object->removeAllAutoIndexes($key[0], $key[1]);

                }else{

                    $modified_object->removeAutoIndex($key[0], $key[1]);

                }
            }
        }

        try{

                $modified_object->store();

            }catch(Exception $e){

                echo "Riak server not available";            

            }    
       
    }


    /*  Function: removeSecondaryIndex
     *  Description: Remove secondary indexes. Receive an object, remove some (or all) secondary indexes from it then save it back on Riak. 
     *              $secondary_indexes must be an array wich contains an array in each position following this form: array(array('index_name', 'index_type', 'index_value')).
     *                  If the contained array has 0 of length, it will remove all indexes.                   
     *                  If the contained array has 1 of length, it will remove all index types for a given index name. 
     *                  If the contained array has 2 of length, it will remove all values from a given index type.
     *                  If the contained array has 3 of length, it will remove an specific index of an specific type with an specific value.
     *              $object is the riak object in wich you want to add one or more primary indexes.
     *              $return_object is a false boolean by default. It determinate if return the object or save it back to Riak.
     */
    /*
    private function removeSecondaryIndex($secondary_indexes, $object){

        $modified_object = $object;

        foreach ($secondary_indexes as $key) {

            $key_length = count($key);
            
            
            if($key_length === 0){

                $modified_object->removeAllIndexes();

            }elseif($key_length === 1){

                $modified_object->removeAllIndexes($key[0]);

            }elseif ($key_length === 2) {
                
                $modified_object->removeAllIndexes($key[0], $key[1]);

            }else{

                $modified_object->removeIndex($key[0], $key[1], $key[2]);

            }

        }

        try{

                $modified_object->store();

            }catch(Exception $e){

                echo "Riak server not available";            

            }    
        
    }
   
   */

    /*  Function: getLinkedObjects
     *  Description: Return an array with all objectsl linked to a given object. 
     *              $object is the riak object from you wanna get the linked objects.
     *              $link_name may be a strings or null.
     *              $bucket may be a string or null.
     *                  if both $link_name and $bucket are null, return all linked objects from the given object.
     */

    public function getLinkedObjects($object, $link_name = null, $bucket = null){

        $response = array();
        $linked_objs = null;

        if(is_null($link_name) && is_null($bucket)){

             $linked_objs = $object->getlinks();

        }else{

            $linked_objs = $object->link($bucket, $link_name)->run();

        }


        foreach ($linked_objs as $key ) {
            
            $response[]=$key->get();

        }

        return $response;

    }


    public function search($bucket, $query){



        /*$selected_bucket = $this->client->bucket('searchbucket');
        echo 'checkpoint1';
        $selected_bucket->newObject("one", array("foo"=>"one", "bar"=>"red"))->store();      
        $selected_bucket->newObject("two", array("foo"=>"two", "bar"=>"green"))->store();
        echo 'checkpoint2';
        */
        # Execute a search for all objects with matching properties
        $results = $this->client->search("searchbucket", "foo:one OR foo:two")->run();

        //$results = $this->client->search($bucket, $query)->run();
        return $results;
        

        //$results = $client->search("searchbucket", "foo:one OR foo:two")->run();
    }


}