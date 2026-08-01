import { Component, computed, inject } from '@angular/core';
import { applyEach, form, FormField, max, min } from '@angular/forms/signals';
import { StateService } from '../../services/state.service';
import { MatSliderModule } from '@angular/material/slider';
import { MatInputModule } from '@angular/material/input';
import { MatIcon } from '@angular/material/icon';
import { MatButton, MatIconButton } from '@angular/material/button';
import { MatToolbar } from '@angular/material/toolbar';

@Component({
  selector: 'pbu-color-form',
  imports: [
    MatSliderModule, MatInputModule, FormField, MatIcon, MatIconButton, MatButton, MatToolbar
  ],
  templateUrl: './color-form.html',
  styleUrl: './color-form.css',
})
export class ColorForm {
  stateService = inject(StateService)
  stateForm = this.stateService.stateForm

  singleColor = computed(()=> this.stateForm.colors.length <= 1);

  addShade = this.stateService.addShade;
  addColor = this.stateService.addColor;
  removeShade = this.stateService.removeShade;
  removeColor = this.stateService.removeColor;
}
