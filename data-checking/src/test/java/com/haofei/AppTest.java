package com.haofei;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @Test
    public void test1(){
        String[] tableList = {"add_conch_flow","add_mail_flow","add_reward_flow","auto_buy_prop_flow","auto_fire_time","bankruptcy_flow","baoji_flow","battery_skill_energy_flow","battery_use_skill_flow","bind_phone_flow","boss_lucky_lottery_flow","boss_user_flow","bought_monthly_card_flow","clear_prop_flow","click_event_flow","combo_gift_flow","conch_common_info_flow","conch_win_prize_flow","confirm_express_flow","continuity_lottery_flow","count_activity","coupon_drop_lottery_flow","create_gang","create_private_table_flow","ctrl_limit_change_flow","ctrl_new_user_change_flow","ctrl_value_flow","dec_equipment_flow","dec_gang_gift_prop_flow","dec_prop_flow","dec_user_gang_gift_prop_flow","delete_conch_reward_record_flow","delete_mail_flow","delete_treature_stage_record_flow","delete_user_conch_record_flow","delete_user_drift_bottle_flow","delete_user_drift_bottle_gm_cmd_flow","delete_user_express_reward_flow","delete_user_records_lucky_purch_flow","diamond_flow","dish_change_flow","dismiss_gang","drop_dragon_flow","drop_pay_prop_flow","early_warning_flow","energy_factor_unlock_flow","energy_flow","energy_newbie_flow","energy_packet_score_flow","energy_red_packet_draw_flow","energy_red_packet_exchange_flow","energy_score_draw_flow","energy_swop_prop_flow","exchange_item","exchange_soul_bead_flow","expired_mail_flow","fastlogin_transfer_flow","first_not_show_bind_flow","fishing_accumulation_flow","fish_flow","gang_contrib_flow","gang_create_cabinet","gang_delete_cabinet","gang_donate","gang_invite_user","gang_kick_user","gang_recv_cabinet","gang_recv_donate","gang_reward_contrib_flow","gather_likes_award_flow","gather_likes_flow","gather_likes_interview_flow","genbu_raffle_flow","get_icon_box_flow","gift_code_flow","gift_event_flow","given_real_card_flow","give_prop_flow","gold_lottery_flow","gold_pool_flow","gold_score_change_pay_gold","gold_score_flow","guess_server_record_flow","guess_server_settle_flow","guess_user_bet_flow","guess_user_room_flow","guess_user_settle_flow","guide_step_flow","handle_repeat_trigger_flow","hitting_fish_drop_flow","icon_box_change_flow","icon_change_flow","inc_equipment_flow","inc_express_goods_flow","inc_gang_gift_prop_flow","inc_prop_flow","inc_user_gang_gift_prop_flow","invite_bind","join_gang","kill_circulation_record","level_up_flow","liveness_flow","live_login_before_flow","load_room_flow","login_before_flow","login_before_h5_flow","login_flow","logout_flow","lottery_flow","lucky_box_flow","luck_turntable_flow","mall_exchange_flow","match_apply_log","match_center_click_flow","match_result_log","match_score_flow","modify_nickname_rec","money_flow","mooch_gift_flow","new_account_battery_flow","new_register_login_rewards_record","online_reward_flow","order_limit_flow","pay_click_flow","pay_order_result_flow","pay_success_flow","pay_value_flow","phoenix_draw_flow","phoenix_exch_flow","phoenix_raffle_flow","player_module_statistics_flow","pool_monitor_log","private_table_enter_flow","private_table_quit_flow","propery_flow","purch_lucky_purch","push_notify_bind_flow","quit_gang","rank_award_flow","receive_reward_conch_flow","recv_expire_prop_rewards","red_packet_change_energy_flow","red_packet_change_gold","red_packet_draw","red_packet_exchange","register_flow","relief_fund_flow","renew_private_table_flow","req_join_gang","result_lucky_purch","return_dec_items_flow","robot_sailing_room_quit_flow","room_enter_flow","room_fish_spawn_flow","room_in_out_flow","room_in_out_fow","room_pool_flow","room_pool_summary","room_quit_flow","room_slots_flow","room_slots_score_flow","room_taxs_today_flow","sand_task_flow","saving_pot_flow","sea_soul_reward_flow","sea_soul_score_flow","second_not_show_bind_flow","share_activity_flow","share_activity_lottery_flow","simple_exchange_item_flow","small_game_award","small_game_circulation","small_game_trigger","spec_fish_record","summon_boss_flow","switch_equipment_flow","take_gift_flow","take_mail_flow","task_flow","task_off_reward_flow","thanos_raffle_flow","touch_fish_flow","treasure_box_flow","treature_punish_activity_reward_flow","two_passwd_flow","update_actvity_number","update_sg_game_income_flow","upgrade_cannon_flow","user_attend_task_off_reward_flow","user_control_drop_flow","user_drift_bottle_flow","user_gang_contrib_flow","user_lottery_tax_flow","user_mall_exchange_flow","user_new_year_buy_flow","user_new_year_extra_flow","user_new_year_free_flow","user_new_year_like_flow","user_sailing_room_quit_flow","user_treasure_sail_flow","vip_daily_gift_flow","vip_raffle_flow","vip_up_flow","welfare_effect_flow","welfare_pool_flow","welfare_score_flow","world_boss_room_flow","world_boss_user_flow"};
        /*while (true){
            System.out.println(tableList[new Random().nextInt(tableList.length)]);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/
    }

    @Test
    public void test2(){
        List<String> list = new ArrayList<>(20);
        for (int i = 0; i < 30; i++) {
            list.add("lo"+i);
        }
        System.out.println(list.toString());
    }
}
