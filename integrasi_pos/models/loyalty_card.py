from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
import logging

_logger = logging.getLogger(__name__)
  

class LoyaltyCard(models.Model):
    _inherit = 'loyalty.card'

    is_integrated = fields.Boolean(string="User created", default=False, readonly=True, tracking=True)
    history_ids = fields.One2many(comodel_name='loyalty.history', inverse_name='card_id', readonly=True)

    @api.model_create_multi
    def create(self, vals_list):
        # Tandai setiap record sebagai terintegrasi
        for vals in vals_list:
            vals['is_integrated'] = True
        # Buat record loyalty.card
        res = super().create(vals_list)
        # Buat riwayat loyalty.history untuk setiap card
        for card in res:
            self.env['loyalty.history'].create({
                'card_id': card.id,
                'points_before': 0,
                'points_after': card.points
            })
        return res
    
    def write(self, vals):
        # Tangani is_integrated
        if vals:
            if 'is_integrated' in vals and vals['is_integrated'] is False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        # Cek dan simpan nilai points sebelum update
        points_before = {}
        if 'points' in vals:
            points_before = {card.id: card.points for card in self}
        # Lanjutkan proses update
        res = super().write(vals)
        # Setelah update, simpan ke loyalty.history jika ada perubahan points
        if 'points' in vals:
            for card in self:
                old = points_before.get(card.id)
                new = card.points
                if old != new:
                    self.env['loyalty.history'].create({
                        'card_id': card.id,
                        'points_before': old,
                        'points_after': new,
                    })
        return res
    
    @api.model
    def get_available_points(self, partner_id):
        """
        Get available loyalty points for a partner
        """
        try:
            # Cari loyalty card untuk partner ini
            loyalty_card = self.search([
                ('partner_id', '=', partner_id),
                ('program_id.active', '=', True)
            ], limit=1, order='points desc')
            
            if loyalty_card:
                return loyalty_card.points
            return 0
        except Exception as e:
            return 0
        
    def redeem_points_immediately(self, points, description=""):
        """
        Deduct points immediately from loyalty card - USING log.note
        """
        try:
            if len(self) != 1:
                return {
                    'success': False,
                    'error': _('Invalid loyalty card')
                }
            
            if self.points < points:
                return {
                    'success': False,
                    'error': _('Insufficient points. Available: %s, Requested: %s') % (self.points, points)
                }
            
            # Update points langsung
            new_balance = self.points - points
            self.write({'points': new_balance})
            
            # ✅ FIX: Gunakan model log.note yang sudah ada
            try:
                self.env['log.note'].create({
                    'vit_doc_type': 'LOYALTY_POINT_REDEMPTION',
                    'vit_trx_key': f'LOYALTY_{self.id}_{fields.Datetime.now().strftime("%Y%m%d_%H%M%S")}',
                    'vit_trx_date': fields.Datetime.now(),
                    'vit_sync_date': fields.Datetime.now(),
                    'vit_sync_status': 'SUCCESS',
                    'vit_sync_desc': f'Points redeemed: -{points} points. Reason: {description}. New balance: {new_balance}. Card: {self.code or self.id}',
                    'vit_start_sync': fields.Datetime.now(),
                    'vit_end_sync': fields.Datetime.now(),
                    'vit_duration': '0s',
                })
                _logger.info(f"Loyalty points redeemed: Card {self.id}, Points: -{points}, New Balance: {new_balance}")
            except Exception as log_error:
                _logger.warning(f"Log note creation failed but continuing: {log_error}")
            
            return {
                'success': True,
                'new_balance': new_balance,
                'message': _('Points deducted successfully')
            }
            
        except Exception as e:
            # Log error ke log.note
            try:
                self.env['log.note'].create({
                    'vit_doc_type': 'LOYALTY_POINT_REDEMPTION_ERROR',
                    'vit_trx_key': f'LOYALTY_{self.id}_{fields.Datetime.now().strftime("%Y%m%d_%H%M%S")}',
                    'vit_trx_date': fields.Datetime.now(),
                    'vit_sync_date': fields.Datetime.now(),
                    'vit_sync_status': 'FAILED',
                    'vit_sync_desc': f'Error redeeming points: {str(e)}. Card: {self.code or self.id}, Points: {points}',
                    'vit_start_sync': fields.Datetime.now(),
                    'vit_end_sync': fields.Datetime.now(),
                    'vit_duration': '0s',
                })
            except:
                pass
                
            _logger.error(f"Error redeeming loyalty points: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def add_points_immediately(self, points, description=""):
        """
        Add points to loyalty card (for rollback) - USING log.note
        """
        try:
            if len(self) != 1:
                return {
                    'success': False,
                    'error': _('Invalid loyalty card')
                }
                
            new_balance = self.points + points
            self.write({'points': new_balance})
            
            # ✅ FIX: Gunakan model log.note untuk rollback logging
            try:
                self.env['log.note'].create({
                    'vit_doc_type': 'LOYALTY_POINT_ROLLBACK',
                    'vit_trx_key': f'LOYALTY_{self.id}_{fields.Datetime.now().strftime("%Y%m%d_%H%M%S")}',
                    'vit_trx_date': fields.Datetime.now(),
                    'vit_sync_date': fields.Datetime.now(),
                    'vit_sync_status': 'SUCCESS',
                    'vit_sync_desc': f'Points rollback: +{points} points. Reason: {description}. New balance: {new_balance}. Card: {self.code or self.id}',
                    'vit_start_sync': fields.Datetime.now(),
                    'vit_end_sync': fields.Datetime.now(),
                    'vit_duration': '0s',
                })
                _logger.info(f"Loyalty points rollback: Card {self.id}, Points: +{points}, New Balance: {new_balance}")
            except Exception as log_error:
                _logger.warning(f"Log note creation failed but continuing: {log_error}")
            
            return {
                'success': True,
                'new_balance': new_balance,
                'message': _('Points added successfully')
            }
            
        except Exception as e:
            # Log error ke log.note
            try:
                self.env['log.note'].create({
                    'vit_doc_type': 'LOYALTY_POINT_ROLLBACK_ERROR',
                    'vit_trx_key': f'LOYALTY_{self.id}_{fields.Datetime.now().strftime("%Y%m%d_%H%M%S")}',
                    'vit_trx_date': fields.Datetime.now(),
                    'vit_sync_date': fields.Datetime.now(),
                    'vit_sync_status': 'FAILED',
                    'vit_sync_desc': f'Error rolling back points: {str(e)}. Card: {self.code or self.id}, Points: {points}',
                    'vit_start_sync': fields.Datetime.now(),
                    'vit_end_sync': fields.Datetime.now(),
                    'vit_duration': '0s',
                })
            except:
                pass
                
            _logger.error(f"Error rolling back loyalty points: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }