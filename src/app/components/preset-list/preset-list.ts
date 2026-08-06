import { Component, computed, inject } from '@angular/core';
import { PresetModel, PresetState } from '../../services/preset.state';
import { ColorState } from '../../services/color.state';
import { PresetCard } from '../preset-card/preset-card';

@Component({
  selector: 'pbu-preset-list',
  imports: [PresetCard],
  templateUrl: './preset-list.html',
  styleUrl: './preset-list.css',
})
export class PresetList {
  colorState = inject(ColorState)
  presetState = inject(PresetState)
  state = this.presetState.state

  customPresets = computed(()=>this.state().custom)
  defaultPresets = computed(()=>this.state().default)
}
