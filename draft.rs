loop {
    // election
    tx = Transaction::with_leader_info_timeout();

    // try to get lease here

    // TODO: This approach is busy wait. We could also use observe
    // https://github.com/risingwavelabs/risingwave/blob/069330b7f740b5224c15252b293a24e444a1ce13/test_HA_setup/src/main.rs#L112
    
    loop {
        sleep(lease_time/2 + random());

        // current term in office 
        leader_info = Meta::get_value();
        
        // elect new leader if there is no leader
        if leader_info == Null {
            break;
        }

        if leader_info.leader == me {
            Meta::prolong_lease();
            continue;
        }

        // leader time is over
        if leader_info.timed_out() {
            Meta::delete_lease();
            break;
        }   
    }

}