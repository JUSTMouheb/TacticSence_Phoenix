import { Component, OnInit } from '@angular/core';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-player-dashboard',
  templateUrl: './player-dashboard.component.html',
  styleUrls: ['./player-dashboard.component.css'] // Changed from .scss to .css
})
export class PlayerDashboardComponent implements OnInit {
  userName: string = '';
  userAvatar: string | null = null;

  constructor(
    private authService: AuthService
  ) { }

  ngOnInit(): void {
    const user = this.authService.getCurrentUserSync();
    if (user) {
      this.userName = user.name;
      this.userAvatar = user.prefs?.avatar || null;
    }
  }

  logout(): void {
    this.authService.logout();
    window.location.href = '/';
  }
}